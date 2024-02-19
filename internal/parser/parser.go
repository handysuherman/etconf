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

func (p *parser) Create(configData map[interface{}]interface{}) error {
	rootKeysYAML, err := p.createKey(configData)
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

func (p *parser) Update(configData map[interface{}]interface{}) error {
	keysToUpdate := strings.Split(p.cfg.UpdateConfigKeys, ",")
	for _, key := range keysToUpdate {
		if err := p.updateKey(configData, key); err != nil {
			return err
		}
	}

	fmt.Println("configuration updated in etcd.")
	return nil
}

func (p *parser) createKey(configData map[interface{}]interface{}) (map[string]interface{}, error) {
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

					if err := util.UpdateKeyContent(p.etcdClient, nvalue, etcdKey, nkey.(string)); err != nil {
						return nil, err
					}

					fmt.Printf("update %s YAML content stored in etcd key: %s\n", key, etcdKey)
					configurationsYAML[nkey.(string)] = etcdKey
				}
			}
		default:
			etcdKey := fmt.Sprintf("%s/%s", p.cfg.EtcdPrefix, strings.ToLower(key.(string)))
			if err := util.UpdateKeyContent(p.etcdClient, value, etcdKey, key.(string)); err != nil {
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

func (p *parser) updateKey(configData map[interface{}]interface{}, key string) error {
	nestedKeys := strings.Split(key, ".")
	if _, ok := configData[nestedKeys[0]]; !ok {
		return fmt.Errorf("specified update key '%s' does not exist", key)
	}

	if len(nestedKeys) >= 2 {
		return p.updateNestedKey(configData, key, nestedKeys)
	}

	return p.updateFlatKey(configData, key)
}

func (p *parser) updateNestedKey(configData map[interface{}]interface{}, key string, nestedKeys []string) error {
	if p.cfg.TLSRootLevel == nestedKeys[0] {
		if tlsYAML, ok := configData[nestedKeys[0]].(map[interface{}]interface{}); ok {
			_, err := p.encodeTLS(tlsYAML, nestedKeys[1], nil)
			return err
		}
		return fmt.Errorf("specified update key '%s' is not under 'tls' root level", key)
	}

	anyYAML, ok := configData[nestedKeys[0]].(map[interface{}]interface{})
	if !ok {
		return fmt.Errorf("specified update key '%s' is not under '%s' root level", key, nestedKeys[0])
	}

	for k, value := range anyYAML {
		if k == nestedKeys[1] {
			etcdKey := fmt.Sprintf("%s/%s/%s", p.cfg.EtcdPrefix, nestedKeys[0], strings.ToLower(k.(string)))
			err := util.UpdateKeyContent(p.etcdClient, value, etcdKey, strings.ToLower(k.(string)))
			if err != nil {
				return err
			}

			fmt.Printf("Updated TLS YAML content stored in etcd key: %s\n", etcdKey)
			return nil
		}
	}

	return fmt.Errorf("specified update key '%s' does not exist", key)
}

func (p *parser) updateFlatKey(configData map[interface{}]interface{}, key string) error {
	if _, ok := configData[key]; ok {
		return fmt.Errorf("specified update key '%s' does not exist", key)
	}

	return util.UpdateKeyContent(p.etcdClient, configData[key], fmt.Sprintf("%s/%s", p.cfg.EtcdPrefix, strings.ToLower(key)), key)
}

func (p *parser) encodeTLS(
	tlsYAML map[interface{}]interface{},
	targetKey string,
	resultYAML map[string]interface{},
) (map[string]interface{}, error) {
	if targetKey == "" {
		for key, service := range tlsYAML {
			etcdKey, value, err := util.Paths(service, p.cfg.EtcdPrefix, p.cfg.TLSRootLevel, key.(string))
			if err != nil {
				return nil, err
			}
			if err := util.UpdateKeyContent(p.etcdClient, value, etcdKey, key.(string)); err != nil {
				return nil, err
			}

			resultYAML[key.(string)] = etcdKey
			fmt.Printf("Updated TLS YAML content stored in etcd key: %s\n", etcdKey)
		}

		return resultYAML, nil
	}

	if service, ok := tlsYAML[targetKey]; ok {
		etcdKey, value, err := util.Paths(service, p.cfg.EtcdPrefix, p.cfg.TLSRootLevel, targetKey)
		if err != nil {
			return nil, err
		}

		if err := util.UpdateKeyContent(p.etcdClient, value, etcdKey, targetKey); err != nil {
			return nil, err
		}

		fmt.Printf("Updated TLS YAML content stored in etcd key: %s\n", etcdKey)
	}

	return resultYAML, nil
}
