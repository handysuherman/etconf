package util

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	timeoutDur = 15 * time.Second
)

func Base64EncodeFile(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("unable to read file content: %v", err)
	}

	return base64.StdEncoding.EncodeToString(content), nil
}

func UpdateToEtcd(client *clientv3.Client, key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeoutDur)
	defer cancel()

	_, err := client.Put(ctx, key, value)
	return err
}
