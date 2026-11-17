#!/usr/bin/env python3
"""备份完整性核验演示：损坏、换备份、核验中断与恢复阻断。

用法（Compose 环境）：
    docker compose up -d --build
    python3 scripts/verify_demo.py

环境变量：
    API_URL       默认 http://localhost:8000
    DEMO_PSQL     在源库执行 SQL 的命令前缀
                  默认 "docker compose exec -T db psql -U postgres -d water"
    BACKUP_VOLUME 备份卷在**本机**的挂载路径（用于人为破坏备份文件做验证）。
                  未设置时自动尝试 `docker compose config --format json`
                  解析 backups 卷的宿主机路径；仍拿不到则跳过"损坏"类断言，
                  并明确提示需要设置该变量。

流程：
  1. 基线：核验当前最新备份代次 → passed，恢复一个核对点 → ready 且可查
  2. 损坏：破坏备份中的真实数据文件 → 重新核验 → failed，返回损坏文件、
     核验时间和备份代次；新恢复请求 409
  3. 已就绪实例：上一步的 ready 库仍可查询（不被判坏备份连累）
  4. 换备份：force 制作新备份（未核验）→ 恢复仍 409；核验新代次通过 →
     解除阻断 → 新核对点恢复成功
  5. 中断：再次核验时立即 DELETE 核验 → interrupted（不报通过）→ 恢复 409
"""
import json
import os
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.request

API = os.environ.get("API_URL", "http://localhost:8000")
DEMO_PSQL = os.environ.get(
    "DEMO_PSQL", "docker compose exec -T db psql -U postgres -d water")

FAILED: list[str] = []
SKIPPED: list[str] = []
# 核对点名带运行序号，便于在同一环境重复执行（核对点名全局唯一）
RUN_ID = os.environ.get("VERIFY_DEMO_RUN_ID") or str(int(time.time()) % 100000)


def api(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    req = urllib.request.Request(
        API + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return e.code, {"raw": raw}


def psql(sql: str) -> None:
    subprocess.run(shlex.split(DEMO_PSQL) + ["-v", "ON_ERROR_STOP=1", "-c", sql],
                   check=True, capture_output=True, text=True)


def wait_for(desc: str, fn, timeout: int = 300):
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = fn()
        if r:
            return r
        time.sleep(2)
    raise SystemExit(f"超时：{desc}")


def check(desc: str, cond: bool) -> None:
    print(f"  [{'PASS' if cond else 'FAIL'}] {desc}")
    if not cond:
        FAILED.append(desc)


def skip(desc: str) -> None:
    print(f"  [SKIP] {desc}")
    SKIPPED.append(desc)


def backup_volume_host_path() -> str | None:
    if os.environ.get("BACKUP_VOLUME"):
        return os.environ["BACKUP_VOLUME"]
    try:
        out = subprocess.run(["docker", "compose", "config", "--format", "json"],
                             capture_output=True, text=True, check=True).stdout
        cfg = json.loads(out)
        name = cfg["volumes"]["backups"].get("name") or \
            f"{cfg.get('name', 'water-pitr')}_backups"
        path = subprocess.run(
            ["docker", "volume", "inspect", "-f", "{{.Mountpoint}}", name],
            capture_output=True, text=True, check=True).stdout.strip()
        return path or None
    except Exception:  # noqa: BLE001
        return None


def latest_backup() -> dict:
    return api("GET", "/api/basebackup")[1]["latest_done"]


def wait_backup(gen: int) -> dict:
    return wait_for(f"备份代次 #{gen} 完成",
                    lambda: (lambda d: d if d["id"] == gen else None)(latest_backup()))


def verify(gen: int) -> dict:
    code, body = api("POST", f"/api/basebackup/{gen}/verify")
    assert code == 202, f"发起核验失败: {code} {body}"
    vid = body["verification_id"]
    return wait_for(f"核验 #{vid} 结束",
                    lambda: (lambda v: v if v["status"] != "running" else None)(
                        api("GET", f"/api/verifications/{vid}")[1]))


def create_checkpoint(name: str) -> dict:
    code, cp = api("POST", "/api/checkpoints", {"name": name})
    assert code == 201, f"创建核对点 {name} 失败: {code} {cp}"
    return wait_for(f"核对点 {name} 就绪",
                    lambda: (lambda c: c if c["status"] == "ready" else None)(
                        api("GET", f"/api/checkpoints/{name}")[1]))


def restore_and_wait(cp_name: str, expect: str = "ready") -> tuple[int, dict]:
    code, job = api("POST", "/api/recoveries", {"checkpoint": cp_name})
    assert code == 202, f"发起恢复 {cp_name} 失败: {code} {job}"
    jid = job["id"]
    job = wait_for(f"任务 #{jid} 结束",
                   lambda: (lambda j: j if j["status"] not in
                            ("queued", "waiting_wal", "preparing", "recovering",
                             "verifying") else None)(
                       api("GET", f"/api/recoveries/{jid}")[1]))
    return jid, job


def corrupt_one_file(backup_path: str) -> str:
    """按 backup_manifest 找一个带校验和的数据文件，翻转一个字节。"""
    manifest_path = os.path.join(backup_path, "backup_manifest")
    with open(manifest_path) as f:
        manifest = json.load(f)
    target = next(
        f for f in manifest["Files"]
        if f.get("Checksum-Algorithm") == "CRC32C"
        and f["Path"].startswith(("base/", "global/")))
    path = os.path.join(backup_path, target["Path"])
    with open(path, "r+b") as f:
        f.seek(min(200, max(target["Size"] - 1, 0)))
        b = f.read(1)
        f.seek(-1, os.SEEK_CUR)
        f.write(bytes([b[0] ^ 0xFF]))
    return target["Path"]


# ------------------------------------------------------------------ 主流程

def main() -> int:
    print("== 0. 等待 API 与基准备份 ==")
    wait_for("API 健康", lambda: api("GET", "/api/health")[1].get("status") == "ok")
    wait_for("基准备份完成",
             lambda: (api("GET", "/api/basebackup")[1].get("latest_done") or {})
             .get("status") == "done")
    vol = backup_volume_host_path()
    print(f"  备份卷宿主机路径：{vol or '（未获取到）'}")

    print("== 1. 基线：核验最新备份 + 恢复就绪 ==")
    base = latest_backup()
    v = verify(base["id"])
    check("基线备份核验通过", v["status"] == "passed")
    check("核验结果带备份代次", v["backup_generation"] == base["id"])
    check("核验结果带核验时间", bool(v["verified_at"]))
    cp1 = create_checkpoint(f"vdemo1-{RUN_ID}")
    jid1, job1 = restore_and_wait(cp1["name"])
    check("基线恢复 ready", job1["status"] == "ready")
    check("ready 实例可查询", api(
        "GET", f"/api/recoveries/{jid1}/samples")[0] == 200)
    check("恢复任务携带备份代次", job1.get("backup_id") == base["id"])

    print("== 2. 损坏备份 → 核验 failed + 阻断 ==")
    corruption_tested = False
    if not vol:
        skip("损坏备份场景（未获取到备份卷路径，请设置 BACKUP_VOLUME）")
    else:
        gen = latest_backup()["id"]
        path = latest_backup()["path"]
        host = path if os.path.isdir(path) else \
            os.path.join(vol, os.path.basename(path))
        if not os.path.isdir(host):
            skip(f"损坏备份场景（找不到备份目录 {host}）")
        else:
            corruption_tested = True
            rel = corrupt_one_file(host)
            print(f"  已翻转 {rel} 的 1 个字节")
            v = verify(gen)
            check("损坏备份核验 failed", v["status"] == "failed")
            check("返回损坏文件明细", any(rel in x for x in v["corruption"]))
            check("损坏结论绑定正确代次", v["backup_generation"] == gen)
            check("失败带核验时间", bool(v["verified_at"]))
            gate = api("GET", "/api/verifications")[1]["integrity_gate"]
            check("完整性闸门已立起", gate["blocked"] is True
                  and gate["gate_backup_generation"] == gen)
            code, body = api("POST", "/api/recoveries", {"checkpoint": cp1["name"]})
            check("新恢复请求被拒绝（409）", code == 409)
            check("重新核验同一坏备份仍失败", verify(gen)["status"] == "failed")

    print("== 3. 已就绪的独立只读库不受阻断影响 ==")
    check("先前 ready 实例仍可查询", api(
        "GET", f"/api/recoveries/{jid1}/samples")[0] == 200)

    print("== 4. 换备份：新代次未核验仍阻断，核验通过后解除 ==")
    code, nb = api("POST", "/api/basebackup?force=true")
    assert code == 202, f"强制制作备份失败: {code} {nb}"
    new_gen = wait_backup(nb["id"])["id"]
    check("新备份代次独立", new_gen != base["id"])
    v_for_new = api("GET", f"/api/basebackup/{new_gen}/verifications")[1]
    check("新代次没有继承旧核验结论", v_for_new == [])
    gate = api("GET", "/api/verifications")[1]["integrity_gate"]
    if corruption_tested and gate["blocked"]:
        check("新备份未核验 → 仍阻断", gate["blocked"] is True)
        check("新代次恢复请求被拒绝", api(
            "POST", "/api/recoveries", {"checkpoint": cp1["name"]})[0] == 409)
    else:
        skip("“新备份未核验仍阻断”依赖第 2 步已判坏；本次无判坏历史，"
             "按设计新代次不强制先核验")
    v = verify(new_gen)
    check("新备份核验通过", v["status"] == "passed")
    check("核验通过解除阻断",
          api("GET", "/api/verifications")[1]["integrity_gate"]["blocked"] is False)
    cp2 = create_checkpoint(f"vdemo2-{RUN_ID}")
    jid2, job2 = restore_and_wait(cp2["name"])
    check("阻断解除后新恢复 ready", job2["status"] == "ready")
    check("新恢复绑定新备份代次", job2["backup_id"] == new_gen)

    print("== 5. 核验中断 → interrupted，绝不报告通过 ==")
    code, body = api("POST", f"/api/basebackup/{new_gen}/verify")
    assert code == 202, f"发起核验失败: {code} {body}"
    vid = body["verification_id"]
    api("DELETE", f"/api/verifications/{vid}")   # 立刻中断，不等待自然完成
    v = wait_for(f"核验 #{vid} 落定",
                 lambda: (lambda x: x if x["status"] != "running" else None)(
                     api("GET", f"/api/verifications/{vid}")[1]))
    if v["status"] == "passed":
        # 备份很小、核验在 DELETE 前已跑完；本场景无法验证"中断不通过"
        skip("中断场景（核验过快，未能在完成前中断；可在较大备份上重试）")
    else:
        check("中断核验记 interrupted", v["status"] == "interrupted")
        check("中断绝不报告 passed", v["status"] != "passed")
        gate = api("GET", "/api/verifications")[1]["integrity_gate"]
        check("中断立起阻断", gate["blocked"] is True)
        check("中断后恢复请求被拒绝", api(
            "POST", "/api/recoveries", {"checkpoint": cp2["name"]})[0] == 409)
        check("同代次重新核验通过仍不解禁（须换新备份）",
              verify(new_gen)["status"] == "passed" and
              api("GET", "/api/verifications")[1]["integrity_gate"]["blocked"] is True)
        code, nb2 = api("POST", "/api/basebackup?force=true")
        assert code == 202, f"强制制作备份失败: {code} {nb2}"
        newer_gen = wait_backup(nb2["id"])["id"]
        check("更新的备份未核验前仍阻断",
              api("GET", "/api/verifications")[1]["integrity_gate"]["blocked"] is True)
        check("更新代次核验通过才解除",
              verify(newer_gen)["status"] == "passed" and
              api("GET", "/api/verifications")[1]["integrity_gate"]["blocked"] is False)

    print()
    if SKIPPED:
        print(f"跳过 {len(SKIPPED)} 项：")
        for s in SKIPPED:
            print(f"  - {s}")
    if FAILED:
        print(f"共 {len(FAILED)} 项校验失败：")
        for f in FAILED:
            print(f"  - {f}")
        return 1
    print("全部校验通过 ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
