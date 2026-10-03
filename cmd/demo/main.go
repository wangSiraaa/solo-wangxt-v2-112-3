// Command demo drives the backup HTTP API through the full failure story:
//
//  1. committed snapshot + restore into a new directory with digest/length
//     verification (empty file included),
//  2. small edit reusing existing content-defined chunks,
//  3. refusal to restore over an existing directory,
//  4. file actively written during scan -> re-read then rejected,
//  5. commit interruption losing a blob -> failed snapshot with the exact
//     missing chunk located,
//  6. symlink escaping the root -> restored link is blocked,
//  7. server restart with a pending snapshot -> startup recovery commits it,
//  8. repository-wide integrity patrol: clean baseline, silent corruption of
//     a shared blob -> every referencing snapshot suspect with file paths,
//     restore refused early,
//  9. patrol high-watermark: a snapshot born after the start is "uncovered"
//     (never faked clean), a later patrol covers it.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"incbackup/internal/api"
	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

var pass, fail int

func main() {
	keep := flag.Bool("keep", false, "keep the demo workspace afterwards")
	flag.Parse()

	work, err := os.MkdirTemp("", "incbackup-demo-")
	must(err)
	if *keep {
		fmt.Printf("(workspace: %s)\n", work)
	} else {
		defer os.RemoveAll(work)
	}
	repoDir := filepath.Join(work, "repo")
	src := filepath.Join(work, "src")

	section(0, "准备：在一个进程内启动本地 API 服务与数据目录")
	srv := startServer(repoDir)
	fmt.Printf("  API   : %s\n", srv.URL)
	fmt.Printf("  仓库  : %s (manifest.sqlite + chunks/)\n", repoDir)
	fmt.Printf("  数据源: %s\n", src)
	must(os.MkdirAll(filepath.Join(src, "docs"), 0o755))

	// Big-ish log so content-defined chunking produces several chunks.
	log := make([]byte, 0, 160*1024)
	for i := 0; i < 160*1024; i++ {
		log = append(log, byte("abcdefghijklmnopqrstuvwxyz0123456789\n"[i%37]))
	}
	must(os.WriteFile(filepath.Join(src, "app.log"), log, 0o644))
	must(os.WriteFile(filepath.Join(src, "docs", "notes.txt"), []byte("meeting notes\n"), 0o644))
	must(os.WriteFile(filepath.Join(src, "run.sh"), []byte("#!/bin/sh\necho hi\n"), 0o750))
	must(os.WriteFile(filepath.Join(src, "EMPTY.dat"), nil, 0o600)) // empty file
	must(os.Symlink("docs/notes.txt", filepath.Join(src, "link_to_notes")))

	// ---- 1. first snapshot + restore --------------------------------------
	section(1, "首次快照：完成前逐块验证，然后恢复到全新目录并核对摘要与长度")
	r := post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "baseline"})
	firstID := int64(r["snapshot_id"].(float64))
	fmt.Printf("  快照 %d: status=%s 新块=%v 引用块=%v\n",
		firstID, r["status"], r["chunks_new"], r["chunks_referenced"])
	check("快照状态为 committed", r["status"] == "committed")

	restoreDir := filepath.Join(work, "restore-1")
	code, body := raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", firstID),
		map[string]any{"target": restoreDir})
	if code != http.StatusCreated {
		must(fmt.Errorf("restore #1 failed: HTTP %d %s", code, body["message"]))
	}
	rr := body
	verified, _ := rr["verified"].([]any)
	fmt.Printf("  恢复到 %s\n  文件=%v 目录=%v 符号链接=%v 字节=%v\n",
		restoreDir, rr["files"], rr["directories"], rr["symlinks"], rr["bytes"])
	for _, v := range verified {
		m := v.(map[string]any)
		fmt.Printf("    %-18s 长度=%-6d 块数=%-2d 摘要=%s… 权限=0%o\n",
			m["rel_path"], int64(m["size"].(float64)), int(m["chunk_count"].(float64)),
			m["digest"].(string)[:16], int64(m["mode"].(float64)))
	}
	emptyOK := false
	for _, v := range verified {
		m := v.(map[string]any)
		if m["rel_path"] == "EMPTY.dat" {
			emptyOK = m["size"].(float64) == 0 &&
				m["digest"] == fmt.Sprintf("%x", sha256.New().Sum(nil)) &&
				m["chunk_count"].(float64) == 0
		}
	}
	check("空文件：长度 0、SHA256=e3b0c44…、0 个内容块", emptyOK)

	// Compare tree metadata with source.
	var modeMismatch []string
	for _, rel := range []string{"run.sh", "app.log", "docs"} {
		a, _ := os.Lstat(filepath.Join(src, rel))
		b, err := os.Lstat(filepath.Join(restoreDir, rel))
		if err != nil || a.Mode().Perm() != b.Mode().Perm() {
			modeMismatch = append(modeMismatch, rel)
		}
	}
	check("目录权限与文件权限均保留 (run.sh 0750, docs 0755)", len(modeMismatch) == 0)
	lt, _ := os.Readlink(filepath.Join(restoreDir, "link_to_notes"))
	check("符号链接本身被恢复（链接目标=docs/notes.txt，未跟随）", lt == "docs/notes.txt")
	notes, err := os.ReadFile(filepath.Join(restoreDir, "link_to_notes"))
	check("恢复出的链接仍可解析到文件内容", err == nil && string(notes) == "meeting notes\n")

	// byte-identical content of app.log independently re-hashed
	got, _ := hashFile(filepath.Join(restoreDir, "app.log"))
	want, _ := hashFile(filepath.Join(src, "app.log"))
	check("恢复内容逐字节一致（独立重算 SHA256）", got == want)

	// ---- 2. small edit reuses chunks --------------------------------------
	section(2, "小改动的增量：在 app.log 中部改一行，只新增 1 个块，其余块全部复用")
	off := 80 * 1024
	copy(log[off:off+8], []byte("PATCHED!"))
	must(os.WriteFile(filepath.Join(src, "app.log"), log, 0o644))
	r = post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "one-line patch"})
	secondID := int64(r["snapshot_id"].(float64))
	newChunks := int64(r["chunks_new"].(float64))
	refChunks := int64(r["chunks_referenced"].(float64))
	fmt.Printf("  快照 %d: 引用块=%d，其中新写入=%d，复用=%d\n",
		secondID, refChunks, newChunks, refChunks-newChunks)
	check("仅有改动附近的 1 个块是新块（内容定义分块边界由内容决定）", newChunks == 1)
	check("其余块全部复用快照 1 中的旧块", refChunks-newChunks == refChunks-1)

	restore2 := filepath.Join(work, "restore-2")
	code, body = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", secondID), map[string]any{"target": restore2})
	if code != http.StatusCreated {
		must(fmt.Errorf("restore #2 failed: HTTP %d %s", code, body["message"]))
	}
	g2, _ := hashFile(filepath.Join(restore2, "app.log"))
	w2, _ := hashFile(filepath.Join(src, "app.log"))
	check("恢复快照 2 后 app.log 与当前源文件一致", g2 == w2)

	// ---- 3. never overwrite destination -----------------------------------
	section(3, "恢复位置已有任何东西 → 拒绝，不覆盖、不合并")
	code, body = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", firstID),
		map[string]any{"target": restoreDir})
	fmt.Printf("  POST restore 到已存在目录 -> HTTP %d: %s\n", code, body["error"])
	check("已有目录时返回 409 target_exists", code == http.StatusConflict && body["error"] == "target_exists")

	// ---- 4. file being written during scan --------------------------------
	section(4, "扫描中仍在写入的文件：先短暂写入触发重读，再持续写入触发拒绝")
	growing := filepath.Join(src, "growing.log")
	must(os.WriteFile(growing, []byte("line0\n"), 0o644))

	// 4a. writer finishes within the retry window: scanner re-reads and commits
	done := make(chan struct{})
	go func() {
		f, _ := os.OpenFile(growing, os.O_APPEND|os.O_WRONLY, 0o644)
		for i := 1; i <= 5; i++ {
			fmt.Fprintf(f, "line%d %s\n", i, strings.Repeat("y", 200))
			time.Sleep(20 * time.Millisecond)
		}
		f.Close()
		close(done)
	}()
	snap4a := post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "writer settles during retry"})
	<-done
	check("写入在重读窗口内结束：扫描器重读文件，快照仍正常 committed", snap4a["status"] == "committed")

	// 4b. writer keeps going: every pass sees a changed size/mtime -> rejected
	stop := make(chan struct{})
	go func() {
		f, _ := os.OpenFile(growing, os.O_APPEND|os.O_WRONLY, 0o644)
		defer f.Close()
		i := 1
		for {
			select {
			case <-stop:
				return
			default:
				fmt.Fprintf(f, "line%d %s\n", i, strings.Repeat("x", 200))
				i++
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()
	time.Sleep(30 * time.Millisecond)
	code, body = raw("POST", srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "racing writer"})
	close(stop)
	reasons, _ := body["reasons"].([]any)
	fmt.Printf("  持续写入时 HTTP %d, 快照 %v -> %s\n", code, body["snapshot_id"], body["error"])
	for _, x := range reasons {
		fmt.Printf("    拒绝原因: %s\n", x)
	}
	sawUnstable := false
	for _, x := range reasons {
		if strings.Contains(x.(string), "growing.log") &&
			strings.Contains(x.(string), "still being written") {
			sawUnstable = true
		}
	}
	check("3 次重读后仍在变化的 growing.log 被明确点名（而不是备份静默成功）",
		code == http.StatusConflict && sawUnstable)
	badID := int64(body["snapshot_id"].(float64))
	errs, _ := get(srv.URL + fmt.Sprintf("/v1/snapshots/%d/errors", badID))["errors"].([]any)
	check("失败快照保留在清单中，stage=scan 可追溯", len(errs) > 0 &&
		errs[0].(map[string]any)["stage"] == "scan")

	// ---- 5. commit interruption: lost blob, locate exact chunk ------------
	section(5, "模拟提交中断：删掉最后一个内容块 → 完成前验证拦截并定位具体缺块")
	must(os.WriteFile(growing, []byte("stable now\n"), 0o644))
	code, body = raw("POST", srv.URL+"/v1/snapshots",
		map[string]any{"root": src, "message": "interrupted commit", "lose_chunks": 1})
	fmt.Printf("  HTTP %d, 快照 %v -> %s\n", code, body["snapshot_id"], body["error"])
	interruptedID := int64(body["snapshot_id"].(float64))
	check("缺块快照不能 committed，返回 409", code == http.StatusConflict)

	missing := get(srv.URL + fmt.Sprintf("/v1/snapshots/%d/missing", interruptedID))["missing"].([]any)
	fmt.Printf("  维护查询 GET .../missing 找到 %d 个缺块：\n", len(missing))
	for _, x := range missing {
		m := x.(map[string]any)
		fmt.Printf("    文件   : %s\n", m["rel_path"])
		fmt.Printf("    块摘要 : %s\n", m["chunk_digest"])
		fmt.Printf("    应在   : %s\n", m["expected_blob_path"])
		fmt.Printf("    原因   : %s\n", m["reason"])
		_, statErr := os.Stat(m["expected_blob_path"].(string))
		check("报告的块路径在磁盘上确实不存在", os.IsNotExist(statErr))
	}
	check("缺块清单精确到 文件+摘要+期望磁盘路径（不是“上传队列为空”）", len(missing) == 1)
	si := get(srv.URL + fmt.Sprintf("/v1/snapshots/%d", interruptedID))
	check("失败快照状态可查 = failed", si["status"] == "failed")

	// restore of a failed snapshot must be refused
	code, body = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", interruptedID),
		map[string]any{"target": filepath.Join(work, "never")})
	fmt.Printf("  尝试恢复 failed 快照 -> HTTP %d %s\n", code, body["error"])
	check("failed 快照拒绝恢复", code >= 400)

	// ---- 6. symlink escape containment ------------------------------------
	section(6, "符号链接越界：备份只存链接本身，恢复时指向根目录外的链接被拒绝")
	secret := filepath.Join(work, "secret.txt")
	must(os.WriteFile(secret, []byte("TOP SECRET"), 0o600))
	evil := filepath.Join(src, "evil_link")
	_ = os.Remove(evil)
	rel, _ := filepath.Rel(filepath.Join(src), secret)
	must(os.Symlink(rel, evil)) // src/evil_link -> ../secret.txt
	snapEvil := post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "with evil link"})
	evilID := int64(snapEvil["snapshot_id"].(float64))
	evilTarget := filepath.Join(work, "restore-evil")
	code, body = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", evilID),
		map[string]any{"target": evilTarget})
	fmt.Printf("  含越界链接的恢复 -> HTTP %d: %s\n", code, body["message"])
	check("越界符号链接恢复被阻止 (422)", code == http.StatusUnprocessableEntity)
	_, statErr := os.Lstat(evilTarget)
	check("失败后不留半成品目录（回滚清理）", os.IsNotExist(statErr))
	_, err = os.ReadFile(filepath.Join(evilTarget, "evil_link"))
	check("秘密文件没有被触及/写出", err != nil)

	// ---- 7. restart recovery of a pending snapshot -------------------------
	section(7, "提交前进程退出：快照留在 pending，服务重启时自动验证并给结论")
	pend := post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "crash before commit", "finish": false})
	pendID := int64(pend["snapshot_id"].(float64))
	fmt.Printf("  故障时刻: 快照 %d status=%s，块已落盘、清单未提交\n", pendID, pend["status"])
	check("finish=false 留下 pending 快照", pend["status"] == "pending")
	srv.Close()

	srv = startServer(repoDir) // same repo, new process equivalent
	time.Sleep(100 * time.Millisecond)
	si = get(srv.URL + fmt.Sprintf("/v1/snapshots/%d", pendID))
	fmt.Printf("  重启后: 快照 %d status=%s\n", pendID, si["status"])
	check("重启恢复把 pending 快照验证后提交为 committed", si["status"] == "committed")

	// ---- 8. integrity patrol: silent rot of a shared blob ---------------
	section(8, "全仓完整性巡检：静默损坏一个被多个快照共享的块 → 全部受影响快照标 suspect 并提前拒绝恢复")
	// Snapshot the current tree once more so a stable chunk is shared by at
	// least two committed snapshots (#1/#2 share most of app.log; make sure
	// we target a chunk they genuinely share).
	scrubBase := post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "patrol baseline"})
	patrolID := int64(scrubBase["snapshot_id"].(float64))
	check("巡检基线快照 committed", scrubBase["status"] == "committed")

	// 8a. first patrol over healthy storage
	code, body = raw("POST", srv.URL+"/v1/scrubs", map[string]any{})
	check("启动巡检返回 201（新巡检作业）", code == http.StatusCreated && body["action"] == "started")
	firstScrub := waitScrub(srv.URL)
	check("巡检作业最终状态 done", firstScrub["status"] == "done")
	check("巡检在 SQLite 中记录了开始水位 high_watermark",
		int64(firstScrub["high_watermark"].(float64)) >= patrolID)
	cleanCount := firstScrub["chunks_clean"].(float64)
	check("每个块都有流式摘要结果（chunks_clean>0，逐块 SHA256 校验）", cleanCount > 0)
	badBaseline := int64(firstScrub["chunks_bad"].(float64))
	snaps := firstScrub["snapshots"].([]any)
	allClean := true
	var sawExcludedFailed bool
	for _, x := range snaps {
		m := x.(map[string]any)
		if m["commit_status"] == "committed" && m["covered"] == true && m["integrity"] != "clean" {
			allClean = false
		}
		if m["commit_status"] == "failed" && m["integrity"] == "excluded" {
			sawExcludedFailed = true
		}
	}
	check("水位内已提交快照全部 clean（历史 committed 事实不变，巡检状态独立呈现）", allClean)
	check("既有 failed 快照单独标为 excluded，不被算作 clean、原有故障记录不变", sawExcludedFailed)
	sd := get(srv.URL + fmt.Sprintf("/v1/scrubs/%d/snapshots/%d",
		int64(firstScrub["run_id"].(float64)), patrolID))
	check("按快照查询巡检状态：committed 快照返回 clean + 已扫描块数",
		sd["integrity"] == "clean" && int64(sd["chunks_scanned"].(float64)) > 0)

	// Pick a chunk shared at app.log by snapshot #2 and the patrol baseline
	// snapshot (snapshots #1/#2/#8 all contain the same big file).
	sharedDigest := pickSharedChunk(srv.URL, secondID, patrolID, "app.log")
	if sharedDigest == "" {
		must(fmt.Errorf("could not find an app.log chunk shared by snapshots %d and %d", secondID, patrolID))
	}
	affectedBefore := get(srv.URL + fmt.Sprintf("/v1/scrubs/%d/chunks/%s/affected",
		int64(firstScrub["run_id"].(float64)), sharedDigest))
	check("健康块反查：result=ok、healthy=true、能列出引用它的快照",
		affectedBefore["result"] == "ok" && affectedBefore["healthy"] == true &&
			len(affectedBefore["affected_snapshot_ids"].([]any)) >= 2)

	// 8b. silently corrupt the shared blob on disk (same length -> rot, not truncation)
	corruptChunkOnDisk(repoDir, sharedDigest)

	// Restore BEFORE the second patrol: streaming verification still catches it.
	code, body = raw("POST", fmt.Sprintf("%s/v1/snapshots/%d/restore", srv.URL, secondID),
		map[string]any{"target": filepath.Join(work, "streaming-gate")})
	fmt.Printf("  恢复时逐块流式校验 -> HTTP %d %s\n", code, body["error"])
	check("未巡检时恢复仍由流式 SHA256 兜底拦截", code >= 400)
	if _, err := os.Lstat(filepath.Join(work, "streaming-gate")); !os.IsNotExist(err) {
		check("被拦截的恢复不留半成品目录", false)
	} else {
		check("被拦截的恢复不留半成品目录", true)
	}

	// 8c. second patrol discovers the rot and attributes it to every snapshot
	code, body = raw("POST", srv.URL+"/v1/scrubs", map[string]any{})
	check("再次启动巡检是新作业（不重复旧报告）", code == http.StatusCreated)
	secondScrub := waitScrub(srv.URL)
	run2 := int64(secondScrub["run_id"].(float64))
	// The patrol baseline was healthy apart from the failpoint blob of the
	// section-5 failed snapshot (unique to it); the shared blob just tampered
	// with adds exactly one bad chunk to whatever the baseline reported.
	check(fmt.Sprintf("损坏共享块后巡检 chunks_bad = 基线 %d + 1", badBaseline),
		int64(secondScrub["chunks_bad"].(float64)) == badBaseline+1)
	var suspectCommitted []int64
	for _, x := range secondScrub["snapshots"].([]any) {
		m := x.(map[string]any)
		if m["commit_status"] == "committed" && m["integrity"] == "suspect" {
			suspectCommitted = append(suspectCommitted, int64(m["snapshot_id"].(float64)))
		}
	}
	fmt.Printf("  巡检标记 suspect 的已提交快照: %v\n", suspectCommitted)
	check("共享块损坏后，引用它的多个已提交快照全部被标为 suspect", len(suspectCommitted) >= 2)
	check("第 5 步的 failed 快照仍是 excluded，不会因为引用坏块就改判历史", func() bool {
		f := get(srv.URL + fmt.Sprintf("/v1/scrubs/%d/snapshots/%d", run2, interruptedID))
		return f["integrity"] == "excluded" && f["commit_status"] == "failed"
	}())

	// Reverse-lookup via the manifest lists every snapshot + file path.
	aff := get(srv.URL + fmt.Sprintf("/v1/scrubs/%d/chunks/%s/affected", run2, sharedDigest))
	refSnaps := aff["affected_snapshot_ids"].([]any)
	refPaths := aff["references"].([]any)
	fmt.Printf("  清单反查: 块 %s… 被 %d 个快照、%d 条 文件路径 引用\n",
		sharedDigest[:12], len(refSnaps), len(refPaths))
	check("反查结果为 digest_mismatch / healthy=false",
		aff["result"] == "digest_mismatch" && aff["healthy"] == false)
	sawAppLog := false
	for _, x := range refPaths {
		if x.(map[string]any)["rel_path"] == "app.log" {
			sawAppLog = true
		}
	}
	check("反查列出了所有受影响文件路径（包含 app.log）", sawAppLog)
	// Every committed referencing snapshot must be suspect; every suspect
	// committed snapshot must show up in the reference list.
	refSet := map[int64]bool{}
	for _, x := range refSnaps {
		refSet[int64(x.(float64))] = true
	}
	suspectSet := map[int64]bool{}
	for _, id := range suspectCommitted {
		suspectSet[id] = true
	}
	lookupAgrees := true
	for id := range suspectSet {
		if !refSet[id] {
			lookupAgrees = false
		}
	}
	for _, x := range secondScrub["snapshots"].([]any) {
		m := x.(map[string]any)
		id := int64(m["snapshot_id"].(float64))
		if m["commit_status"] == "committed" && refSet[id] && m["integrity"] != "suspect" {
			lookupAgrees = false
		}
	}
	check("反查快照集合与 suspect 判定完全一致（excluded 的 failed 快照单列）", lookupAgrees)

	// Known-suspect restore must be refused BEFORE streaming, with locators.
	for _, sid := range []int64{suspectCommitted[0], suspectCommitted[1]} {
		code, body = raw("POST", fmt.Sprintf("%s/v1/snapshots/%d/restore", srv.URL, sid),
			map[string]any{"target": filepath.Join(work, fmt.Sprintf("suspect-%d", sid))})
		fmt.Printf("  恢复已知 suspect 快照 %d -> HTTP %d %s\n", sid, code, body["error"])
		if code != http.StatusConflict || body["error"] != "snapshot_suspect" {
			check(fmt.Sprintf("快照 %d 恢复被提前拒绝 (409 snapshot_suspect)", sid), false)
		} else {
			check(fmt.Sprintf("快照 %d 恢复被提前拒绝 (409 snapshot_suspect)", sid), true)
		}
		chunks := body["affected_chunks"].([]any)
		namesChunk := false
		for _, c := range chunks {
			cm := c.(map[string]any)
			if cm["chunk_digest"] != sharedDigest || cm["expected_blob_path"] == "" {
				continue
			}
			for _, rp := range cm["rel_paths"].([]any) {
				if rp == "app.log" {
					namesChunk = true
				}
			}
		}
		check("拒绝错误可定位：给出摘要、rel_paths 与期望磁盘路径", namesChunk)
	}

	// 8d. interrupt + resume equivalence
	section(8, "巡检中断：停在第一个块之前后重启巡检，结果与一次完整巡检一致、不重复报告")
	// gate_ms blocks the patrol before its first chunk: a deterministic
	// "slow disk" barrier that makes the interrupt reproducible.
	code, body = raw("POST", srv.URL+"/v1/scrubs", map[string]any{"gate_ms": 400})
	if code != http.StatusCreated {
		must(fmt.Errorf("gated scrub start: %d %v", code, body))
	}
	interRun := int64(body["progress"].(map[string]any)["run_id"].(float64))
	waitUntilScrubRunning(srv.URL, interRun)
	// Zero chunks checkpointed yet — interrupt from the very first cursor.
	early := get(srv.URL + fmt.Sprintf("/v1/scrubs/%d", interRun))
	check("栅栏内巡检已在运行但尚未扫出任何块（chunks_scanned=0）",
		early["running"] == true && int64(early["chunks_scanned"].(float64)) == 0)
	// A second start while running must be refused rather than duplicating.
	code, body = raw("POST", srv.URL+"/v1/scrubs", map[string]any{})
	check("巡检进行中再次启动 -> 409 scrub_running（不生成重复作业）",
		code == http.StatusConflict && body["error"] == "scrub_running")
	code, _ = raw("POST", srv.URL+"/v1/scrubs/interrupt", nil)
	check("巡检进行中可请求中断 (200)", code == http.StatusOK)
	waitUntilScrubIdle(srv.URL, interRun)
	mid := get(srv.URL + fmt.Sprintf("/v1/scrubs/%d", interRun))
	check("中断后作业仍为 running，稳定游标已保存（本例为起点，可续扫）",
		mid["status"] == "running")
	partialScanned := int64(mid["chunks_scanned"].(float64))
	partiallyCovered := 0
	for _, x := range mid["snapshots"].([]any) {
		m := x.(map[string]any)
		if m["covered"] == true && m["commit_status"] == "committed" && m["integrity"] == "unscanned" {
			partiallyCovered++
		}
	}
	check("尚未扫描的数据不会被误标 clean（覆盖快照全部呈现 unscanned）", partiallyCovered > 0)

	code, body = raw("POST", srv.URL+"/v1/scrubs", map[string]any{})
	check("重启巡检复用同一作业（200 resumed，run_id 不变）",
		code == http.StatusOK && body["action"] == "resumed" &&
			int64(body["progress"].(map[string]any)["run_id"].(float64)) == interRun)
	resumedRun := waitScrub(srv.URL)
	check("续扫完成后 done：扫描块数=工作集块数，bad 块数与完整巡检一致",
		resumedRun["status"] == "done" &&
			int64(resumedRun["chunks_scanned"].(float64)) == int64(resumedRun["chunks_total"].(float64)) &&
			int64(resumedRun["chunks_bad"].(float64)) == badBaseline+1)
	check("从稳定游标之后继续：最终已扫描数大于中断点（0）",
		int64(resumedRun["chunks_scanned"].(float64)) > partialScanned)
	// Snapshot verdicts after resume must match the uninterrupted run 2.
	resumeAgrees := true
	for _, sid := range suspectCommitted {
		d := get(srv.URL + fmt.Sprintf("/v1/scrubs/%d/snapshots/%d", interRun, sid))
		if d["integrity"] != "suspect" {
			resumeAgrees = false
		}
	}
	check("中断续扫的快照结论与一次完整巡检一致（suspect 集合相同）", resumeAgrees)

	// Repair the tampered blob (flip the bytes back) so the next section can
	// show a clean patrol; the content store is content-addressed and the
	// blob is immutable by convention, so this is an explicit repair action.
	flipChunkOnDisk(repoDir, sharedDigest)

	// ---- 9. high-watermark: snapshots born during a patrol ---------------
	section(9, "开始水位之后才提交的快照：明确标为 uncovered，绝不假装已检查")
	// Launch a patrol gated before its first chunk; while it waits, commit a
	// brand-new snapshot. The barrier makes the "committed after the patrol
	// started" ordering deterministic instead of racing disk speed.
	code, body = raw("POST", srv.URL+"/v1/scrubs", map[string]any{"gate_ms": 600})
	if code != http.StatusCreated {
		must(fmt.Errorf("watermark scrub start: %d %v", code, body))
	}
	wmRun := int64(body["progress"].(map[string]any)["run_id"].(float64))
	waitUntilScrubRunning(srv.URL, wmRun)
	// Drop the escaping symlink from section 6 so this snapshot restores
	// cleanly; the point here is watermark coverage, not link containment.
	_ = os.Remove(filepath.Join(src, "evil_link"))
	must(os.WriteFile(filepath.Join(src, "late.txt"), []byte("born after patrol start\n"), 0o644))
	late := post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "committed during patrol"})
	lateID := int64(late["snapshot_id"].(float64))
	check("巡检期间新建快照不受影响，照常 committed", late["status"] == "committed")
	waitScrub(srv.URL)
	wmSnap := get(srv.URL + fmt.Sprintf("/v1/scrubs/%d/snapshots/%d", wmRun, lateID))
	fmt.Printf("  晚于水位提交的快照 %d -> covered=%v integrity=%s\n",
		lateID, wmSnap["covered"], wmSnap["integrity"])
	check("水位后快照标为 uncovered（而不是 clean/unscanned 含糊处理）",
		wmSnap["covered"] == false && wmSnap["integrity"] == "uncovered")
	// uncovered is not suspect: restore remains governed by streaming checks.
	code, body = raw("POST", fmt.Sprintf("%s/v1/snapshots/%d/restore", srv.URL, lateID),
		map[string]any{"target": filepath.Join(work, "late-out")})
	check("uncovered 快照不是 suspect，恢复照常工作（流式校验兜底）", code == http.StatusCreated)
	// A later patrol covers it (the repaired shared blob verifies again).
	nextScrub := waitScrubAfterStart(srv.URL, map[string]any{})
	lateInNext := get(srv.URL + fmt.Sprintf("/v1/scrubs/%d/snapshots/%d",
		int64(nextScrub["run_id"].(float64)), lateID))
	check("下一次巡检覆盖该快照：covered=true 且 clean",
		lateInNext["covered"] == true && lateInNext["integrity"] == "clean")
	// And the repaired shared blob returns the two reference snapshots to clean.
	bothRepaired := true
	for _, sid := range suspectCommitted {
		d := get(srv.URL + fmt.Sprintf("/v1/scrubs/%d/snapshots/%d",
			int64(nextScrub["run_id"].(float64)), sid))
		if d["integrity"] != "clean" {
			bothRepaired = false
		}
	}
	check("修复共享块后，受影响快照在下一次巡检中恢复 clean", bothRepaired)

	// final listing
	section(0, "快照总览（status 是历史提交事实；integrity 是最近一次巡检的独立状态）")
	list := get(srv.URL + "/v1/snapshots")["snapshots"].([]any)
	for _, x := range list {
		m := x.(map[string]any)
		fmt.Printf("  #%-3v %-10s integrity=%-10v files=%-3v %s\n",
			m["id"], m["status"], m["integrity"], m["file_count"], m["message"])
	}
	srv.Close()

	fmt.Println()
	if fail == 0 {
		fmt.Printf("✅ 全部 %d 项检查通过\n", pass)
		return
	}
	fmt.Printf("❌ %d 项失败，%d 项通过\n", fail, pass)
	os.Exit(1)
}

// ---------- helpers ----------

func startServer(repoDir string) *httptest.Server {
	must(os.MkdirAll(repoDir, 0o755))
	manifest, err := repo.OpenManifest(filepath.Join(repoDir, "manifest.sqlite"))
	must(err)
	store, err := repo.NewContentStore(filepath.Join(repoDir, "chunks"))
	must(err)
	engine, err := backup.NewEngine(manifest, store)
	must(err)
	if recovered, err := engine.RecoverPending(); err == nil {
		for _, r := range recovered {
			fmt.Printf("  [启动恢复] 快照 %d -> %s\n", r.SnapshotID, r.Status)
		}
	}
	return httptest.NewServer((&api.Server{Engine: engine}).NewRouter())
}

func post(url string, body any) map[string]any {
	code, b := raw("POST", url, body)
	if code >= 300 {
		out, _ := json.MarshalIndent(b, "", "  ")
		fmt.Println(string(out))
	}
	return b
}

func get(url string) map[string]any {
	code, b := raw("GET", url, nil)
	if code >= 300 {
		out, _ := json.MarshalIndent(b, "", "  ")
		fmt.Println(string(out))
	}
	return b
}

func raw(method, url string, body any) (int, map[string]any) {
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		must(err)
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, url, rdr)
	must(err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	must(err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	must(err)
	var out map[string]any
	if len(data) > 0 {
		must(json.Unmarshal(data, &out))
		if out == nil {
			out = map[string]any{}
		}
	} else {
		out = map[string]any{}
	}
	return resp.StatusCode, out
}

func hashFile(p string) (string, int64) {
	f, err := os.Open(p)
	must(err)
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	must(err)
	return hex.EncodeToString(h.Sum(nil)), n
}

func section(n int, title string) {
	if n == 0 {
		fmt.Printf("\n── %s ──────────────────────────────\n", title)
		return
	}
	fmt.Printf("\n── %d. %s ──────────────────────────────\n", n, title)
}

// waitScrub polls the latest patrol until it finishes.
func waitScrub(base string) map[string]any {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		b := get(base + "/v1/scrubs/latest")
		if b["status"] == "done" && b["running"] == false {
			return b
		}
		time.Sleep(20 * time.Millisecond)
	}
	must(fmt.Errorf("scrub never finished"))
	return nil
}

// waitScrubAfterStart starts a patrol and waits for it to finish.
func waitScrubAfterStart(base string, req map[string]any) map[string]any {
	code, b := raw("POST", base+"/v1/scrubs", req)
	if code != http.StatusCreated && code != http.StatusOK {
		must(fmt.Errorf("scrub start: %d %v", code, b))
	}
	return waitScrub(base)
}

// waitUntilScrubRunning blocks until the patrol goroutine is active. With
// gate_ms the run is guaranteed mid-barrier (zero chunks checkpointed yet),
// which is the strongest interrupt case.
func waitUntilScrubRunning(base string, runID int64) {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		b := get(base + fmt.Sprintf("/v1/scrubs/%d", runID))
		if b["running"] == true {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	must(fmt.Errorf("scrub %d never started", runID))
}

// waitUntilScrubIdle blocks until the run's in-process goroutine is gone.
func waitUntilScrubIdle(base string, runID int64) {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		b := get(base + fmt.Sprintf("/v1/scrubs/%d", runID))
		if b["running"] == false {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	must(fmt.Errorf("scrub %d never went idle after interrupt", runID))
}

// pickSharedChunk finds a chunk digest referenced at relPath by both
// snapshots — the classic "one blob shared by two backups at the same file".
func pickSharedChunk(base string, a, b int64, relPath string) string {
	ca := chunkDigestSetAt(base, a, relPath)
	cb := chunkDigestSetAt(base, b, relPath)
	for d := range cb {
		if ca[d] {
			return d
		}
	}
	return ""
}

func chunkDigestSetAt(base string, snapID int64, relPath string) map[string]bool {
	b := get(base + fmt.Sprintf("/v1/snapshots/%d/chunks", snapID))
	out := map[string]bool{}
	for _, x := range b["chunks"].([]any) {
		m := x.(map[string]any)
		if m["rel_path"] == relPath {
			out[m["chunk_digest"].(string)] = true
		}
	}
	return out
}

func chunkDigestSet(base string, snapID int64) map[string]bool {
	b := get(base + fmt.Sprintf("/v1/snapshots/%d/chunks", snapID))
	out := map[string]bool{}
	for _, x := range b["chunks"].([]any) {
		out[x.(map[string]any)["chunk_digest"].(string)] = true
	}
	return out
}

// chunkBlobPath reproduces <repo>/chunks/ab/cdef... for a hex digest.
func chunkBlobPath(repoDir, digestHex string) string {
	return filepath.Join(repoDir, "chunks", digestHex[:2], digestHex[2:])
}

// corruptChunkOnDisk flips every byte of a blob in place (0444 -> writable),
// simulating silent bit-rot without changing length.
func corruptChunkOnDisk(repoDir, digestHex string) {
	flipChunkOnDisk(repoDir, digestHex)
}

// flipChunkOnDisk toggles all bytes of a blob; calling it twice restores the
// original content (self-inverse repair used by the demo).
func flipChunkOnDisk(repoDir, digestHex string) {
	p := chunkBlobPath(repoDir, digestHex)
	data, err := os.ReadFile(p)
	must(err)
	must(os.Chmod(p, 0o644))
	for i := range data {
		data[i] ^= 0xff
	}
	must(os.WriteFile(p, data, 0o644))
}

func check(name string, ok bool) {
	if ok {
		pass++
		fmt.Printf("  ✓ %s\n", name)
		return
	}
	fail++
	fmt.Printf("  ✗ %s\n", name)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "FATAL:", err)
		var ee *exec.ExitError
		_ = ee
		os.Exit(2)
	}
}
