package bpf

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// BenchmarkWithBPF 使用 eBPF 监控 fadvis 并验证其效果
func BenchmarkWithBPF(b *testing.B) {
	// 检查是否以 root 权限运行
	if os.Geteuid() != 0 {
		b.Skip("需要 root 权限才能运行 eBPF 测试")
	}

	// 准备测试文件
	tempFile, err := os.CreateTemp("", "fadvis-bpf-test")
	if err != nil {
		b.Fatalf("创建临时文件失败: %v", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// 写入一些数据
	dataSize := 100 * 1024 * 1024 // 100MB
	data := make([]byte, 1024)
	for i := 0; i < dataSize/1024; i++ {
		if _, err := tempFile.Write(data); err != nil {
			b.Fatalf("写入数据失败: %v", err)
		}
	}
	tempFile.Sync()

	// 启动 eBPF 监控
	cleanup, err := RunFadvise()
	if err != nil {
		b.Fatalf("启动 eBPF 监控失败: %v", err)
	}
	defer cleanup()

	// 运行基准测试
	b.Run("WithFadviseNormal", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// 模拟有 fadvis 的场景
			testReadWithFadvis(b, tempFile)
		}
	})

	b.Run("WithoutFadvise", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// 模拟无 fadvis 的场景
			testReadWithoutFadvis(b, tempFile)
		}
	})

	// 另一种方法：使用 MonitorFadviseWithTimeout
	b.Run("MonitorWithTimeout", func(b *testing.B) {
		// 设置监控时长
		duration := 5 * time.Second
		b.ResetTimer()

		// 启动一个协程执行 fadvis 操作
		go func() {
			for i := 0; i < 100; i++ {
				// 触发 fadvis 操作
				err := fadvis(tempFile, 0, int64(dataSize), FADV_NORMAL)
				if err != nil {
					b.Errorf("fadvis 失败: %v", err)
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
		}()

		// 监控 fadvise64 调用
		stats, err := MonitorFadviseWithTimeout(duration)
		if err != nil {
			b.Fatalf("监控失败: %v", err)
		}

		// 输出统计信息
		b.Logf("监控结果 (%v):", duration)
		for _, stat := range stats {
			b.Logf("PID: %d, 调用次数: %d", stat.PID, stat.Count)
		}

		// 验证是否检测到 fadvis 调用
		if len(stats) == 0 {
			b.Error("未检测到任何 fadvis 调用")
		}
	})
}

func testReadWithFadvis(b *testing.B, file *os.File) {
	// 重置文件指针到开头
	if _, err := file.Seek(0, 0); err != nil {
		b.Fatalf("重置文件指针失败: %v", err)
	}

	// 使用 FADV_SEQUENTIAL 提示
	if err := fadvis(file, 0, 0, FADV_SEQUENTIAL); err != nil {
		b.Fatalf("fadvis 失败: %v", err)
	}

	// 读取文件内容
	buffer := make([]byte, 4096)
	for {
		n, err := file.Read(buffer)
		if err != nil || n == 0 {
			break
		}
	}
}

func testReadWithoutFadvis(b *testing.B, file *os.File) {
	// 重置文件指针到开头
	if _, err := file.Seek(0, 0); err != nil {
		b.Fatalf("重置文件指针失败: %v", err)
	}

	// 读取文件内容，不使用 fadvis
	buffer := make([]byte, 4096)
	for {
		n, err := file.Read(buffer)
		if err != nil || n == 0 {
			break
		}
	}
}

// 输出监控结果的辅助函数
func PrintFadviseMonitoringResults(stats []FadviseStats) string {
	result := "fadvise64 系统调用监控结果:\n"

	if len(stats) == 0 {
		return result + "未检测到任何 fadvis 调用\n"
	}

	for _, stat := range stats {
		result += fmt.Sprintf("PID: %d, 调用次数: %d\n", stat.PID, stat.Count)
	}

	return result
}
