package parser

import (
	"fmt"
	"strings"

	"github.com/handysuherman/etconf/pkg/util"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v2"
)

type updateParser struct {
	cfg        *Config
	etcdClient *clientv3.Client
}

func NewUpdateParser(
	cfg *Config,
	etcdClient *clientv3.Client,
) *parser {
	return &parser{
		cfg:        cfg,
		etcdClient: etcdClient,
	}
}

func (p *updateParser) Parse(configData map[interface{}]interface{}) error {
	keysToUpdate := strings.Split(p.cfg.UpdateConfigKeys, ",")
	for _, key := range keysToUpdate {
		if err := p.updateKey(configData, key); err != nil {
			return err
		}
	}

	fmt.Println("configuration updated in etcd.")
	return nil
}

func (p *updateParser) updateKey(configData map[interface{}]interface{}, key string) error {
	nestedKeys := strings.Split(key, ".")
	if _, ok := configData[nestedKeys[0]]; !ok {
		return fmt.Errorf("specified update key '%s' does not exist", key)
	}

	if len(nestedKeys) >= 2 {
		return p.updateNestedKey(configData, key, nestedKeys)
	}

	return p.updateFlatKey(configData, key)
}

func (p *updateParser) updateNestedKey(configData map[interface{}]interface{}, key string, nestedKeys []string) error {
	if p.cfg.TLSRootLevel == nestedKeys[0] {
		return p.updateTLSKey(configData, key, nestedKeys[1])
	}

	anyYAML, ok := configData[nestedKeys[0]].(map[interface{}]interface{})
	if !ok {
		return fmt.Errorf("specified update key '%s' is not under '%s' root level", key, nestedKeys[0])
	}

	for k, value := range anyYAML {
		if k == nestedKeys[1] {
			return p.updateKeyContent(value, fmt.Sprintf("%s/%s/%s", p.cfg.EtcdPrefix, nestedKeys[0], strings.ToLower(k.(string))), key)
		}
	}

	return fmt.Errorf("specified update key '%s' does not exist", key)
}

func (p *updateParser) updateFlatKey(configData map[interface{}]interface{}, key string) error {
	if _, ok := configData[key]; ok {
		return fmt.Errorf("specified update key '%s' does not exist", key)
	}

	return p.updateKeyContent(configData[key], fmt.Sprintf("%s/%s", p.cfg.EtcdPrefix, strings.ToLower(key)), key)
}

func (p *updateParser) updateTLSKey(configData map[interface{}]interface{}, key, nestedKey string) error {
	tlsYAML, ok := configData[key].(map[interface{}]interface{})
	if !ok {
		return fmt.Errorf("specified update key '%s' s not under '%s' root level", key, p.cfg.TLSRootLevel)
	}

	return p.updateKeyContent(tlsYAML[nestedKey], fmt.Sprintf("%s/%s/%s", p.cfg.EtcdPrefix, p.cfg.TLSRootLevel, strings.ToLower(nestedKey)), key)
}

func (p *updateParser) updateKeyContent(value interface{}, etcdKey, key string) error {
	contentData, err := yaml.Marshal(map[interface{}]interface{}{key: value})
	if err != nil {
		return err
	}

	if err := util.UpdateToEtcd(p.etcdClient, etcdKey, string(contentData)); err != nil {
		return err
	}

	fmt.Printf("updated %s YAML content stored in etcd key: %s\n", key, etcdKey)
	return nil
}
