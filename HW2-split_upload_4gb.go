// main.go
package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/0gfoundation/0g-storage-client/cmd"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	FragmentSize = 400 * 1024 * 1024 // 400 MB
	FragmentNum  = 10                // 目标切成 10 片
)

var (
	rpcURL     string // 0G Chain RPC
	privateKey string // 私钥（不带0x）
	filePath   string // 要上传的 4GB 文件路径
	indexerURL string // indexer 地址，推荐使用
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "split-upload-4g",
		Short: "将 4GB 文件切分成 10 个 400MB 分片并使用 0g-storage-client 上传/下载",
		Run: func(c *cobra.Command, args []string) {
			if err := run(); err != nil {
				logrus.Fatal(err)
			}
		},
	}

	rootCmd.Flags().StringVar(&rpcURL, "rpc", "https://rpc.0g.ai", "0G Chain RPC URL")
	rootCmd.Flags().StringVar(&privateKey, "key", "", "私钥（必填）")
	rootCmd.Flags().StringVar(&filePath, "file", "", "要上传的 4GB 文件路径（必填）")
	rootCmd.Flags().StringVar(&indexerURL, "indexer", "https://indexer.0g.ai", "0G Storage Indexer URL")
	rootCmd.MarkFlagRequired("key")
	rootCmd.MarkFlagRequired("file")

	if err := rootCmd.Execute(); err != nil {
		logrus.Fatal(err)
	}
}

func run() error {
	// 1. 计算原始文件 MD5（后面用来校验）
	originMD5, err := fileMD5(filePath)
	if err != nil {
		return err
	}
	fmt.Printf("原始文件 MD5: %s\n", originMD5)

	// 2. 创建临时目录存放分片
	tmpDir, err := os.MkdirTemp("", "0g-split-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir) // 结束后自动清理

	// 3. 切分文件
	fragmentFiles, err := splitFile(filePath, tmpDir, FragmentSize)
	if err != nil {
		return err
	}
	fmt.Printf("成功切分成 %d 个分片，每个约 400MB\n", len(fragmentFiles))

	// 4. 上传每个分片，收集 root
	var roots []string
	for i, frag := range fragmentFiles {
		fmt.Printf("\n[%d/%d] 正在上传分片: %s\n", i+1, len(fragmentFiles), filepath.Base(frag))

		root, err := uploadSingleFragment(frag)
		if err != nil {
			return fmt.Errorf("上传分片 %d 失败: %w", i+1, err)
		}
		roots = append(roots, root)
		fmt.Printf("分片 %d 上传成功，root = %s\n", i+1, root)
	}

	fmt.Printf("\n=== 所有分片上传完成 ===\n")
	for i, r := range roots {
		fmt.Printf("分片 %02d root: %s\n", i+1, r)
	}

	// 5. 下载 + 合并
	mergedFile := filePath + ".restored"
	if err := downloadAndMerge(roots, mergedFile); err != nil {
		return err
	}

	// 6. 校验 MD5
	restoredMD5, _ := fileMD5(mergedFile)
	fmt.Printf("\n恢复文件 MD5: %s\n", restoredMD5)
	if originMD5 == restoredMD5 {
		fmt.Println("MD5 校验通过！文件 100% 完整恢复")
	} else {
		fmt.Println("MD5 校验失败！")
	}

	return nil
}

// ==================== 工具函数 ====================

// 把大文件切成固定大小的分片（最后一个可能小一点）
func splitFile(src string, dstDir string, chunkSize int64) ([]string, error) {
	f, err := os.Open(src)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, _ := f.Stat()
	var files []string

	buf := make([]byte, chunkSize)
	for i := 0; ; i++ {
		n, err := f.Read(buf)
		if n == 0 {
			if err != nil && err.Error() != "EOF" {
				return nil, err
			}
			break
		}

		fragPath := filepath.Join(dstDir, fmt.Sprintf("fragment_%03d.dat", i))
		out, err := os.Create(fragPath)
		if err != nil {
			return nil, err
		}
		if _, err := out.Write(buf[:n]); err != nil {
			out.Close()
			return nil, err
		}
		out.Close()
		files = append(files, fragPath)

		if err != nil && err.Error() == "EOF" {
			break
		}
	}
	return files, nil
}

// 上传单个分片（复用 0g-storage-client 原生的 upload 命令逻辑）
func uploadSingleFragment(file string) (string, error) {
	// 构造一个临时的 cobra.Command，复用官方的 upload 逻辑
	uploadCmd := cmd.GetUploadCmd() // 0g-storage-client 暴露的函数（新版本都有）

	// 重置 flags（防止残留）
	uploadCmd.Flags().VisitAll(func(f *cobra.Flag) {
		f.Changed = false
	})

	// 关键参数（和命令行完全等价）
	args := []string{
		"--url", rpcURL,
		"--key", privateKey,
		"--file", file,
		"--indexer", indexerURL,
		"--fragment-size", fmt.Sprintf("%d", FragmentSize), // 关键！强制 400MB 分片
		"--expected-replica", "1",
		"--skip-tx", "false", // 每次都发链上交易，确保 root 被记录
		"--timeout", "30m",
	}

	// 临时捕获日志输出，只取 root
	old := logrus.GetLevel()
	logrus.SetLevel(logrus.ErrorLevel) // 静默
	defer logrus.SetLevel(old)

	// 使用一个 channel 来捕获 root
	rootChan := make(chan string, 1)
	origRun := uploadCmd.Run
	uploadCmd.Run = func(cmd *cobra.Command, args []string) {
		// 临时替换 logrus.Info 输出
		origInfo := logrus.Infof
		logrus.Infof = func(format string, args ...interface{}) {
			s := fmt.Sprintf(format, args...)
			if len(s) > 12 && s[:12] == "file uploaded" {
				// 提取 root
				fields := filepath.SplitList(s)
				for _, f := range fields {
					if len(f) == 64 || len(f) == 66 { // 0x + 64 字符
						rootChan <- f
						return
					}
				}
			}
			origInfo(format, args...)
		}
		origRun(cmd, args)
	}

	err := uploadCmd.ExecuteCobra(uploadCmd, args) // 部分版本叫 Execute
	// 上面这行在新版客户端里可能是：uploadCmd.Execute()
	// 如果报错，可改成：uploadCmd.SetArgs(args); uploadCmd.Execute()

	if err != nil {
		return "", err
	}

	select {
	case root := <-rootChan:
		return root, nil
	case <-time.After(60 * time.Second):
		return "", fmt.Errorf("超时未捕获到 root")
	}
}

// 下载 + 合并
func downloadAndMerge(roots []string, outputPath string) error {
	out, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer out.Close()

	for i, root := range roots {
		fmt.Printf("[%d/%d] 正在下载 root: %s\n", i+1, len(roots), root)

		downloadCmd := cmd.GetDownloadCmd() // 同样复用官方 download 命令

		tmpFile, _ := os.CreateTemp("", "0g-download-*.dat")
		tmpPath := tmpFile.Name()
		tmpFile.Close()
		defer os.Remove(tmpPath)

		args := []string{
			"--url", rpcURL,
			"--indexer", indexerURL,
			"--root", root,
			"--output", tmpPath,
			"--timeout", "20m",
		}

		downloadCmd.SetArgs(args)
		if err := downloadCmd.Execute(); err != nil {
			return fmt.Errorf("下载 root %s 失败: %w", root, err)
		}

		// 追加到最终文件
		data, _ := os.ReadFile(tmpPath)
		if _, err := out.Write(data); err != nil {
			return err
		}
		fmt.Printf("分片 %d 下载完成，%d bytes\n", i+1, len(data))
	}
	return nil
}

func fileMD5(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}