"""备份完整性核验：pg_verifybackup 检查已完成备份及其自带 WAL。

关键保证：
- 一次核验只绑定一个备份代次（backups.id）和该代次 backup_manifest 的指纹；
  强制重做备份不会把旧核验结论归到新备份上；
- 只有 pg_verifybackup 正常退出（exit 0）、输出无错误且 manifest 未变才记 passed；
- 进程被杀、API 重启、请求取消、输出不完整一律记 interrupted，绝不报通过；
- failed / interrupted 会立起"完整性闸门"：被阻断代次及更早备份不可用于新恢复，
  进行中的恢复在发布 ready 前会被拦下；必须换一份更新的备份并核验通过才解除；
- 已经 ready 的独立只读实例不受影响，仍可查询。
"""
import hashlib
import json
import os
import signal
import subprocess
import threading
import time

from . import state

PG_VERIFYBACKUP = os.getenv("PG_VERIFYBACKUP", "pg_verifybackup")

_OUTPUT_LIMIT = 64 * 1024
_CORRUPTION_LIMIT = 200

# 闸门变更与"恢复发布 ready"共用同一把锁，保证线性化：
# 要么先发布 ready（成为已就绪实例），要么先记录失败（该任务不得 ready）。
_GATE_LOCK = threading.RLock()

# 进行中的核验：verification_id -> 状态
_running: dict[int, dict] = {}
_start_lock = threading.Lock()


class VerifyError(RuntimeError):
    """请求本身不合法（备份不存在/未完成/已有核验在跑等）。"""


# ---------------------------------------------------------------- 工具

def manifest_sha256(backup_path: str) -> str | None:
    path = os.path.join(backup_path, "backup_manifest")
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def _parse_corruption(output: str) -> list[str]:
    """从 pg_verifybackup 输出中提取损坏/校验失败明细。"""
    bad = []
    for line in output.splitlines():
        s = line.strip()
        low = s.lower()
        if "error:" in low or "checksum mismatch" in low or "mismatch" in low:
            bad.append(s)
            if len(bad) >= _CORRUPTION_LIMIT:
                break
    return bad


def _public(v: dict) -> dict:
    """核验记录的对外视图（附备份代次、核验时间、损坏信息）。"""
    try:
        corruption = json.loads(v["corruption"]) if v.get("corruption") else []
    except (TypeError, json.JSONDecodeError):
        corruption = []
    return {
        "verification_id": v["id"],
        "backup_generation": v["backup_id"],
        "backup_id": v["backup_id"],
        "status": v["status"],
        "started_at": v["started_at"],
        "verified_at": v["finished_at"],
        "finished_at": v["finished_at"],
        "manifest_sha256": v.get("manifest_sha256"),
        "corruption": corruption,
        "error": v.get("error"),
        "output": v.get("output"),
    }


def get_verification(vid: int) -> dict | None:
    v = state.one_row("SELECT * FROM verifications WHERE id=?", (vid,))
    return _public(v) if v else None


def list_all(limit: int) -> list[dict]:
    return [_public(v) for v in state.all_rows(
        "SELECT * FROM verifications ORDER BY id DESC LIMIT ?", (limit,))]


def list_for_backup(backup_id: int) -> list[dict]:
    return [_public(v) for v in state.all_rows(
        "SELECT * FROM verifications WHERE backup_id=? ORDER BY id DESC",
        (backup_id,))]


def latest_for_backup(backup_id: int) -> dict | None:
    v = state.one_row(
        "SELECT * FROM verifications WHERE backup_id=? ORDER BY id DESC LIMIT 1",
        (backup_id,),
    )
    return _public(v) if v else None


# ---------------------------------------------------------------- 闸门

def _gate() -> dict | None:
    """当前闸门：{"backup_id": 被阻断代次, "verification_id": 造成阻断的核验}。"""
    raw = state.get_meta(state.GATE_KEY)
    if raw is None:
        return None
    try:
        data = json.loads(raw)
        return {"backup_id": int(data["backup_id"]),
                "verification_id": int(data["verification_id"])}
    except (ValueError, KeyError, TypeError):
        return None


def _blocked_verification(gate: dict) -> dict | None:
    row = state.one_row(
        "SELECT * FROM verifications WHERE id=?", (gate["verification_id"],))
    return _public(row) if row else None


def _set_gate(backup_id: int, verification_id: int) -> None:
    """立闸：代次更高者优先，同代取更晚的核验；旧失败不会被更旧代次结论覆盖。"""
    cur = _gate()
    if cur is None or backup_id > cur["backup_id"] or (
            backup_id == cur["backup_id"] and verification_id > cur["verification_id"]):
        state.set_meta(state.GATE_KEY, json.dumps(
            {"backup_id": backup_id, "verification_id": verification_id}))


def _gate_satisfied_locked(gate: dict) -> bool:
    """闸门之后是否已有"有效通过"可解禁。

    有效通过 = passed 且核验 id 晚于造成阻断的核验（旧结论不能解禁），
    且代次**高于**被阻断代次：必须换一份新备份并重新核验，才可解除阻断。
    """
    row = state.one_row(
        "SELECT id FROM verifications WHERE status='passed' AND id>?"
        " AND backup_id>? ORDER BY id DESC LIMIT 1",
        (gate["verification_id"], gate["backup_id"]),
    )
    return bool(row)


def _refresh_gate_locked() -> dict | None:
    """若闸门之后已有有效通过，解除闸门；返回当前闸门。"""
    gate = _gate()
    if gate is None:
        return None
    if _gate_satisfied_locked(gate):
        state.delete_meta(state.GATE_KEY)
        return None
    return gate


def _latest_done() -> dict | None:
    return state.one_row(
        "SELECT * FROM backups WHERE status='done' ORDER BY id DESC LIMIT 1")


def gate_status() -> dict:
    with _GATE_LOCK:
        gate = _refresh_gate_locked()
        if gate is None:
            return {"blocked": False, "gate_backup_generation": None,
                    "required_action": None, "verification": None}
        backup = _latest_done()
        if backup and backup["id"] > gate["backup_id"]:
            required = (f"请对更新的备份代次 #{backup['id']} 执行核验 "
                        f"POST /api/basebackup/{backup['id']}/verify；"
                        f"通过后才解除阻断")
        else:
            required = ("请制作新基准备份（POST /api/basebackup?force=true）"
                        "并对新备份执行核验，通过后解除阻断")
        return {
            "blocked": True,
            "gate_backup_generation": gate["backup_id"],
            "required_action": required,
            "verification": _blocked_verification(gate),
        }


def ensure_recovery_allowed() -> None:
    """发起新恢复前的检查；被阻断时抛 VerifyError。"""
    with _GATE_LOCK:
        gate = _refresh_gate_locked()
        if gate is None:
            return
        v = _blocked_verification(gate)
        detail = (v or {}).get("error") or "损坏"
        backup = _latest_done()
        if not backup:
            raise VerifyError(
                f"备份代次 #{gate['backup_id']} 完整性核验未通过（{detail}），"
                f"当前无可用备份，恢复请求被拒绝")
        if backup["id"] <= gate["backup_id"]:
            raise VerifyError(
                f"备份代次 #{gate['backup_id']} 完整性核验未通过（{detail}），"
                f"恢复请求被拒绝；请制作新基准备份并核验通过后再恢复")
        raise VerifyError(
            f"备份代次 #{gate['backup_id']} 完整性核验未通过（{detail}），"
            f"新备份代次 #{backup['id']} 尚未重新核验通过，恢复请求被拒绝；"
            f"请先 POST /api/basebackup/{backup['id']}/verify")


def _generation_blocked(backup_id: int) -> bool:
    """该代次最近一次核验是 failed/interrupted（未重新核验通过）。

    这是"逐代"的永久判断：即使闸门已被更高代次的新备份解除，
    绑定了该坏代次的未完成恢复也绝不能发布 ready。
    """
    row = state.one_row(
        "SELECT status FROM verifications WHERE backup_id=? ORDER BY id DESC LIMIT 1",
        (backup_id,),
    )
    return bool(row) and row["status"] in ("failed", "interrupted")


def assert_job_backup_usable(backup_id: int | None) -> None:
    """任务运行中的检查；任务所用备份代次被判坏则抛 VerifyError。"""
    if not backup_id:
        return
    with _GATE_LOCK:
        _refresh_gate_locked()
        if _generation_blocked(int(backup_id)):
            raise VerifyError(
                f"备份代次 #{backup_id} 完整性核验未通过，该恢复不得发布为 ready")
        gate = _gate()
        if gate is None:
            return
        if int(backup_id) <= gate["backup_id"]:
            raise VerifyError(
                f"备份代次 #{backup_id} 完整性核验未通过，该恢复不得发布为 ready")
        if not _gate_satisfied_locked(gate):
            raise VerifyError(
                f"备份代次 #{backup_id} 尚未重新核验通过，该恢复不得发布为 ready")


class ready_guard:
    """发布 ready 时持有闸门锁：与核验失败记录互斥，杜绝"判坏后仍 ready"。"""

    def __init__(self, backup_id: int | None):
        self.backup_id = backup_id

    def __enter__(self):
        _GATE_LOCK.acquire()
        try:
            assert_job_backup_usable(self.backup_id)
        except Exception:
            _GATE_LOCK.release()
            raise
        return self

    def __exit__(self, exc_type, exc, tb):
        _GATE_LOCK.release()
        return False


# ---------------------------------------------------------------- 启动/停止

def recover_after_restart() -> None:
    """API 启动：上次未结束的核验一律记 interrupted 并立闸，绝不当作通过。"""
    for v in state.all_rows("SELECT * FROM verifications WHERE status='running'"):
        _kill_pid(v.get("pid"))
        _finish(v, "interrupted",
                error="核验被中断（API 重启或进程终止），未完成，不报告通过")
    with _GATE_LOCK:
        _refresh_gate_locked()


def interrupt_all(reason: str) -> None:
    """API 关停：终止进行中的 pg_verifybackup 并记 interrupted。"""
    with _start_lock:
        items = list(_running.items())
    for vid, info in items:
        info["cancel"] = True
        _terminate(info.get("proc"))
        _kill_pid(info.get("pid"))
        v = state.one_row("SELECT * FROM verifications WHERE id=?", (vid,))
        if v and v["status"] == "running":
            _finish(v, "interrupted", error=f"核验被中断：{reason}，不报告通过")


# ---------------------------------------------------------------- 发起核验

def start_verification(backup_id: int) -> dict:
    backup = state.one_row("SELECT * FROM backups WHERE id=?", (backup_id,))
    if not backup:
        raise VerifyError(f"备份代次 #{backup_id} 不存在")
    if backup["status"] != "done":
        raise VerifyError(
            f"备份代次 #{backup_id} 状态为 {backup['status']}，只有已完成备份才能核验")
    path = backup["path"]
    if not os.path.isdir(path):
        raise VerifyError(f"备份代次 #{backup_id} 的目录不存在：{path}")
    manifest = manifest_sha256(path)
    if not manifest:
        raise VerifyError(f"备份代次 #{backup_id} 缺少 backup_manifest，无法核验")
    with _start_lock:
        if _running:
            raise VerifyError(
                f"已有核验任务 #{next(iter(_running))} 正在进行，请等待或先中断")
        running = state.one_row("SELECT id FROM verifications WHERE status='running'")
        if running:
            raise VerifyError(f"已有核验任务 #{running['id']} 正在进行")
        vid = state.execute(
            "INSERT INTO verifications (backup_id, status, started_at, manifest_sha256)"
            " VALUES (?,?,?,?)",
            (backup_id, "running", state.now(), manifest),
        )
        info = {"cancel": False, "proc": None, "pid": None}
        _running[vid] = info
    threading.Thread(target=_run, args=(vid, path, manifest, info),
                     name=f"verify-{vid}", daemon=True).start()
    return get_verification(vid)


def cancel_verification(vid: int) -> dict:
    v = state.one_row("SELECT * FROM verifications WHERE id=?", (vid,))
    if not v:
        raise VerifyError(f"核验任务 #{vid} 不存在")
    if v["status"] != "running":
        return _public(v)
    with _start_lock:
        info = _running.get(vid)
    if info:
        info["cancel"] = True
        _terminate(info.get("proc"))
        _kill_pid(info.get("pid"))
    # 若进程未能在短暂窗口内退出，直接如实落 interrupted（不等待"自然完成"）
    if not (info and info.get("proc")):
        _finish_by_id(vid, "interrupted",
                      error="核验被中断（用户取消），未完成，不报告通过")
    # 等待执行线程落定（最多几秒），让接口返回真实的终态；绝不等它"自然通过"
    for _ in range(30):
        v = state.one_row("SELECT * FROM verifications WHERE id=?", (vid,))
        if not v or v["status"] != "running":
            break
        time.sleep(0.1)
    if v and v["status"] == "running":
        _finish_by_id(vid, "interrupted",
                      error="核验被中断（用户取消），未完成，不报告通过")
        v = state.one_row("SELECT * FROM verifications WHERE id=?", (vid,))
    return _public(v) if v else {}


# ---------------------------------------------------------------- 执行

def _run(vid: int, path: str, manifest: str, info: dict) -> None:
    proc = None
    wal_dir = os.path.join(path, "pg_wal")
    cmd = [PG_VERIFYBACKUP, "-w", wal_dir, path]
    if info.get("cancel"):
        with _start_lock:
            _running.pop(vid, None)
        _finish_by_id(vid, "interrupted",
                      error="核验被中断（用户取消），未完成，不报告通过")
        return
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            start_new_session=True,   # 独立进程组，便于连同包装脚本的子进程一起终止
        )
        info["proc"] = proc
        info["pid"] = proc.pid
        state.execute("UPDATE verifications SET pid=? WHERE id=?", (proc.pid, vid))
        output, _ = proc.communicate()
    except Exception as e:  # noqa: BLE001 - 无法启动也如实失败
        with _start_lock:
            _running.pop(vid, None)
        if info.get("cancel"):
            _finish_by_id(vid, "interrupted",
                          error="核验被中断（用户取消/服务停止），未完成，不报告通过")
        else:
            _finish_by_id(vid, "failed", error=f"无法执行 pg_verifybackup：{e}",
                          output=None)
        return
    else:
        with _start_lock:
            _running.pop(vid, None)

    output = output or ""
    if info.get("cancel"):
        _finish_by_id(vid, "interrupted", output=output,
                      error="核验被中断（用户取消/服务停止），未完成，不报告通过")
        return
    if proc.returncode != 0:
        _finish_by_id(vid, "failed", output=output,
                      error=f"pg_verifybackup 退出码 {proc.returncode}，备份损坏或无法校验")
        return
    # 防交错：核验期间备份目录/manifest 被替换，旧结论不得算作通过
    if manifest_sha256(path) != manifest:
        _finish_by_id(vid, "interrupted", output=output,
                      error="核验期间 backup_manifest 发生变化（备份被替换/删除），"
                            "本次结论作废，不报告通过")
        return
    bad = _parse_corruption(output)
    if bad:
        _finish_by_id(vid, "failed", output=output,
                      error="pg_verifybackup 输出包含错误，按损坏处理",
                      corruption=bad)
        return
    _finish_by_id(vid, "passed", output=output)


def _terminate(proc) -> None:
    """终止 pg_verifybackup 整个进程组（避免 shell 包装层留下子进程）。"""
    if not proc or proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except (OSError, ProcessLookupError):
        try:
            proc.terminate()
        except Exception:  # noqa: BLE001
            pass


def _kill_pid(pid) -> None:
    """终止遗留的核验进程；校验 /proc 命令行，避免误伤复用 PID 的进程。"""
    if not pid:
        return
    try:
        pid = int(pid)
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            cmdline = f.read().decode("utf-8", "replace")
        markers = ("pg_verifybackup", os.path.basename(PG_VERIFYBACKUP))
        if not any(m and m in cmdline for m in markers):
            return
        # 核验进程以 start_new_session 启动，本身即进程组长，连子进程一起收掉
        if os.getpgid(pid) == pid:
            os.killpg(pid, signal.SIGTERM)
        else:
            os.kill(pid, signal.SIGTERM)
    except (OSError, ValueError):
        pass


def _finish_by_id(vid: int, status: str, error: str | None = None,
                  output: str | None = None,
                  corruption: list[str] | None = None) -> None:
    v = state.one_row("SELECT * FROM verifications WHERE id=?", (vid,))
    if not v:
        return
    if v["status"] != "running":
        return  # 已被取消接口/启动恢复抢先落定，绝不覆盖成通过
    _finish(v, status, error=error, output=output, corruption=corruption)


def _finish(v: dict, status: str, error: str | None = None,
            output: str | None = None,
            corruption: list[str] | None = None) -> None:
    """落定核验结果并同步闸门（失败/中断立闸，通过只解除更旧闸门）。"""
    if output is None:
        output = v.get("output")
    if output and len(output) > _OUTPUT_LIMIT:
        output = output[-_OUTPUT_LIMIT:]
    if corruption is None and status in ("failed", "interrupted"):
        corruption = _parse_corruption(output or "")
    with _GATE_LOCK:
        state.execute(
            "UPDATE verifications SET status=?, finished_at=?, error=?, output=?,"
            " corruption=? WHERE id=?",
            (status, state.now(), error, output,
             json.dumps(corruption or [], ensure_ascii=False), v["id"]),
        )
        if status == "passed":
            _refresh_gate_locked()
        else:
            _set_gate(int(v["backup_id"]), int(v["id"]))
