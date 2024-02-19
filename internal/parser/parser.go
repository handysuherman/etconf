package parser

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/handysuherman/etconf/pkg/util"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v2"
)

// Config hold configurations of commandline
type Config struct {
	TLSRootLevel      string `mapstructure:"tls-root-level"`
	DatabaseRootLevel string `mapstructure:"db-root-level"`
	UpdateConfigKeys  string `mapstructure:"update-keys"`
	EtcdTLSEnabled    bool   `mapstructure:"etcd-tls-enabled"`
	EtcdHosts         string `mapstructure:"etcd-hosts"`
	EtcdCaPath        string `mapstructure:"etcd-ca-cert"`
	EtcdCertPath      string `mapstructure:"etcd-cert"`
	EtcdKeyPath       string `mapstructure:"etcd-key"`
	EtcdPrefix        string `mapstructure:"etcd-prefix"`
	Output            bool   `mapstructure:"output"`
	OutputFilePath    string `mapstructure:"output-file-path"`
}

type parser struct {
	cfg        *Config
	etcdClient *clientv3.Client
}

func NewParser(
	cfg *Config,
	etcdClient *clientv3.Client,
) *parser {
	return &parser{
		cfg:        cfg,
		etcdClient: etcdClient,
	}
}

func (p *parser) Parse(configData map[interface{}]interface{}) error {
	rootKeysYAML, err := p.create(configData, p.etcdClient)
	if err != nil {
		return fmt.Errorf("error unmarshalling YAML content: %v", err)
	}

	encodedRootKeysYAML, err := yaml.Marshal(rootKeysYAML)
	if err != nil {
		return fmt.Errorf("error marshalling root keys YAML content: %v", err)
	}

	if p.cfg.Output {
		rootKeysOutputPath := filepath.Join(p.cfg.OutputFilePath)
		if err := os.WriteFile(rootKeysOutputPath, encodedRootKeysYAML, 0644); err != nil {
			return fmt.Errorf("error writing root keys output file: %v", err)
		}
	}

	fmt.Println("configuration uploaded to etcd.")
	return nil
}

func (p *parser) create(configData map[interface{}]interface{}, etcdClient *clientv3.Client) (map[string]interface{}, error) {
	configurationsYAML := make(map[string]interface{})
	tlsConfigYAML := make(map[string]interface{})

	for key, value := range configData {
		switch key {
		case p.cfg.TLSRootLevel:
			if tlsYAML, ok := value.(map[interface{}]interface{}); ok {
				cfg, err := p.encodeTLS(tlsYAML, "", tlsConfigYAML)
				if err != nil {
					return nil, err
				}
				tlsConfigYAML = cfg
			}
		case p.cfg.DatabaseRootLevel:
			if dbYAML, ok := value.(map[interface{}]interface{}); ok {
				for nkey, nvalue := range dbYAML {
					etcdKey := fmt.Sprintf("%s/%s/%s", p.cfg.EtcdPrefix, strings.ToLower(key.(string)), strings.ToLower(nkey.(string)))

					if err := p.updateKeyContent(nvalue, etcdKey, nkey.(string)); err != nil {
						return nil, err
					}

					fmt.Printf("update %s YAML content stored in etcd key: %s\n", key, etcdKey)
					configurationsYAML[nkey.(string)] = etcdKey
				}
			}
		default:
			etcdKey := fmt.Sprintf("%s/%s", p.cfg.EtcdPrefix, strings.ToLower(key.(string)))
			if err := p.updateKeyContent(value, etcdKey, key.(string)); err != nil {
				return nil, err
			}

			fmt.Printf("update %s YAML content stored in etcd key: %s\n", key, etcdKey)
			configurationsYAML[key.(string)] = etcdKey
		}
	}

	etcdCa, err := util.Base64EncodeFile(p.cfg.EtcdCaPath)
	if err != nil {
		return nil, fmt.Errorf("error while encoding etcd ca: %v", err)
	}

	etcdCert, err := util.Base64EncodeFile(p.cfg.EtcdCertPath)
	if err != nil {
		return nil, fmt.Errorf("error while encoding etcd cert: %v", err)
	}

	etcdKey, err := util.Base64EncodeFile(p.cfg.EtcdKeyPath)
	if err != nil {
		return nil, fmt.Errorf("error while encoding etcd key: %v", err)
	}

	rootKeysYAML := map[string]interface{}{
		"etcd": map[string]interface{}{
			"hosts":  strings.Split(p.cfg.EtcdHosts, ","),
			"prefix": p.cfg.EtcdPrefix,
			"keys": map[string]interface{}{
				"configurations": configurationsYAML,
				"tls":            tlsConfigYAML,
			},
			"tls": map[string]interface{}{
				"enabled": p.cfg.EtcdTLSEnabled,
				"ca":      etcdCa,
				"cert":    etcdCert,
				"key":     etcdKey,
			},
		},
	}

	return rootKeysYAML, nil
}

func (p *parser) encodeTLS(
	tlsYAML map[interface{}]interface{},
	targetKey string,
	resultYAML map[string]interface{},
) (map[string]interface{}, error) {
	if targetKey == "" {
		for key, service := range tlsYAML {
			etcdKey, err := p.paths(service, key.(string))
			if err != nil {
				return nil, err
			}
			resultYAML[key.(string)] = etcdKey
			fmt.Printf("Updated TLS YAML content stored in etcd key: %s\n", etcdKey)
		}
	}

	if service, ok := tlsYAML[targetKey]; ok {
		etcdKey, err := p.paths(service, targetKey)
		if err != nil {
			return nil, err
		}
		resultYAML[targetKey] = etcdKey
		fmt.Printf("Updated TLS YAML content stored in etcd key: %s\n", etcdKey)
	}

	return resultYAML, nil
}

func (p *parser) paths(
	value interface{},
	key string,
) (string, error) {
	servicePaths, ok := value.(map[interface{}]interface{})
	if !ok {
		return "", fmt.Errorf("invalid YAML structure for service")
	}

	for pathKey, path := range servicePaths {
		pathStr, ok := path.(string)
		if !ok {
			return "", fmt.Errorf("invalid YAML structure for path %s", pathKey)
		}

		encodedContent, err := util.Base64EncodeFile(pathStr)
		if err != nil {
			return "", err
		}

		servicePaths[pathKey] = encodedContent
	}

	etcdKey := fmt.Sprintf("%s/%s/%s", p.cfg.EtcdPrefix, p.cfg.TLSRootLevel, key)
	if err := p.updateKeyContent(servicePaths, etcdKey, key); err != nil {
		return "", err
	}

	return etcdKey, nil
}

func (p *parser) updateKeyContent(value interface{}, etcdKey, key string) error {
	contentData, err := yaml.Marshal(map[interface{}]interface{}{key: value})
	if err != nil {
		return err
	}

	if err := util.UpdateToEtcd(p.etcdClient, etcdKey, string(contentData)); err != nil {
		return err
	}

	return nil
}
