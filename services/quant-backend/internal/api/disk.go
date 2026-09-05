// 磁盘水位查询（Linux）。
package api

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

// syscallStatfs 文件系统容量快照。
type syscallStatfs struct {
	blocks uint64
	bfree  uint64
}

// diskRejectPct 全量任务拒绝水位，默认 90，测试可用 QUANT_DISK_REJECT_PCT 覆盖。
func diskRejectPct() int {
	v := strings.TrimSpace(os.Getenv("QUANT_DISK_REJECT_PCT"))
	if v == "" {
		return 90
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 || n > 100 {
		return 90
	}
	return n
}

// statfsRoot 查询当前目录所在磁盘。
func statfsRoot(fs *syscallStatfs) error {
	var st syscall.Statfs_t
	if err := syscall.Statfs(".", &st); err != nil {
		return err
	}
	fs.blocks = st.Blocks
	fs.bfree = st.Bavail
	return nil
}
