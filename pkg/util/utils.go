package util

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v2"
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

func Paths(
	value interface{},
	etcdPrefix string,
	tlsRootLevel string,
	key string,
) (string, map[interface{}]interface{}, error) {
	servicePaths, ok := value.(map[interface{}]interface{})
	if !ok {
		return "", nil, fmt.Errorf("invalid YAML structure for service")
	}

	for pathKey, path := range servicePaths {
		pathStr, ok := path.(string)
		if !ok {
			return "", nil, fmt.Errorf("invalid YAML structure for path %s", pathKey)
		}

		encodedContent, err := Base64EncodeFile(pathStr)
		if err != nil {
			return "", nil, err
		}

		servicePaths[pathKey] = encodedContent
	}

	etcdKey := fmt.Sprintf("%s/%s/%s", etcdPrefix, tlsRootLevel, key)
	return etcdKey, servicePaths, nil
}

func UpdateKeyContent(etcdClient *clientv3.Client, value interface{}, etcdKey, key string) error {
	contentData, err := yaml.Marshal(map[interface{}]interface{}{key: value})
	if err != nil {
		return err
	}

	if err := UpdateToEtcd(etcdClient, etcdKey, string(contentData)); err != nil {
		return err
	}

	return nil
}
