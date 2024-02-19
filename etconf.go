package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/handysuherman/etconf/internal/parser"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v2"
)

// Config hold configurations of commandline
type Config struct {
	// location of yaml file that will be parsed by the commandline,
	// this flag is Required.
	YamlPath string `mapstructure:"yaml-file"`

	// yaml configuration level of TLS
	// iterate depth 1 of this configuration for each different setup or etcd key.
	// it will read the provided tls path, and transform or encode it to base64 string.
	// this flag is Required.
	TLSRootLevel string `mapstructure:"tls-root-level"`

	// yaml configuration level of Databases part
	// Iterate depth 1 of this configuration for each different setup or etcd key.
	// This flag is Required.
	DatabaseRootLevel string `mapstructure:"db-root-level"`

	// update defines if this operations an update or not
	// if this flag was enabled, its just update or upload the specified key from UpdateConfigKeys to etcd.
	// if UpdateConfigKeys was empty, it updates all configurations.
	Update bool `mapstructure:"update"`

	// list of desired config keys to be updated.
	// UpdateConfigKeys will only be readed, if update flag was enabled.
	// its possible to specify multiple update keys,
	// eg: databases.mariadb,tls.kafka,tls.mariadb.
	UpdateConfigKeys string `mapstructure:"update-keys"`

	// defines if this operations produce an output etcd config file or not.
	// but takes no effect if you enabled the Update flag.
	// default: false
	Output bool `mapstructure:"output"`

	// specify the path or filename of etcd config file.
	// default: './etcd-config.yaml'
	OutputFilePath string `mapstructure:"output-file-path"`

	// defines if the destination etcd hosts was in TLS connection or not.
	// default: false
	EtcdTLSEnabled bool `mapstructure:"etcd-tls-enabled"`

	// defines destination hosts of etcd.
	// its possible to specify multiple host,
	// eg. http://localhost:2379,http://localhost:2380.
	EtcdHosts string `mapstructure:"etcd-hosts"`

	// defines ca path of the etcd
	// this is required when EtcdTLSEnabled was true.
	EtcdCaPath string `mapstructure:"etcd-ca-cert"`

	// defines cert path of the etcd
	// this is required when EtcdTLSEnabled was true.
	EtcdCertPath string `mapstructure:"etcd-cert"`

	// defines key path of the etcd
	// this is required when EtcdTLSEnabled was true.
	EtcdKeyPath string `mapstructure:"etcd-key"`

	// defines prefix key for etcd
	// default: config.
	// result would be somekind a like: /config/databases/redis and so on.
	EtcdPrefix string `mapstructure:"etcd-prefix"`
}

const (
	defaultEtcdPrefix = "config"
	defaultTimeout    = 10 * time.Second
	yamlType          = "yaml"
)

func main() {
	config := &Config{}

	configFilePath := flag.String("config-file", "", "path to the YAML config file")
	help := flag.Bool("help", false, "show the usage")
	flag.StringVar(&config.YamlPath, "yaml-file", "", "path to the YAML file")
	flag.StringVar(&config.TLSRootLevel, "tls-root-level", "", "root level TLS configuration of the YAML file")
	flag.StringVar(&config.DatabaseRootLevel, "db-root-level", "", "root level Database configuration of the YAML file")
	flag.BoolVar(&config.EtcdTLSEnabled, "etcd-tls-enabled", false, "either TLS enabled in etcd or not, default: false")
	flag.StringVar(&config.EtcdHosts, "etcd-hosts", "", "etcd destinations remote hosts, may specify multiple hosts, e.g., http://localhost:2379,http//localhost:2380")
	flag.StringVar(&config.EtcdCaPath, "etcd-ca-cert", "", "path to etcd CA cert file")
	flag.StringVar(&config.EtcdCertPath, "etcd-cert", "", "path to etcd cert file")
	flag.StringVar(&config.EtcdKeyPath, "etcd-key", "", "path to etcd key file")
	flag.StringVar(&config.EtcdPrefix, "etcd-prefix", defaultEtcdPrefix, "etcd prefix, default: config, it would be like /config/app, and so on.")
	flag.BoolVar(&config.Update, "update", false, "Update specific config keys")
	flag.StringVar(&config.UpdateConfigKeys, "update-keys", "", "specify the configuration keys to Update when --Update flag is provided")
	flag.BoolVar(&config.Output, "output", false, "return an output etcd config yaml file, if you are doing an Update, there will be no output file even this flag was set to true.")
	flag.StringVar(&config.OutputFilePath, "output-file-path", "etcd-config.yaml", "specify a filename of an outputfile, if not specified, set to default: etcd-config.yaml")
	flag.Parse()

	if *help {
		flag.Usage()
		return
	}

	if *configFilePath != "" {
		viper.SetConfigType(yamlType)
		viper.SetConfigFile(*configFilePath)

		if err := viper.ReadInConfig(); err != nil {
			fmt.Printf("viper.ReadInConfig.err: %v", err)
			return
		}

		if err := viper.Unmarshal(config); err != nil {
			fmt.Printf("viper.Unmarshal.err: %v", err)
			return
		}
	}

	if err := validateFlags(config); err != nil {
		fmt.Printf("error occured when validating flags: %v\n", err)
		fmt.Print("try --help to see more information\n")
		os.Exit(1)
		return
	}

	exec := new(config)
	if err := exec.run(); err != nil {
		fmt.Printf("error occured when executing: %v\n", err)
		return
	}
}

type config struct {
	cfg        *Config
	etcdClient *clientv3.Client
}

func new(cfg *Config) *config {
	return &config{
		cfg: cfg,
	}
}

func (exec *config) run() error {
	etcdHosts := strings.Split(exec.cfg.EtcdHosts, ",")

	etcdCfg, err := createEtcdConfig(exec.cfg)
	if err != nil {
		return fmt.Errorf("error creating etcd config: %v", err)
	}

	etcdCfg.Endpoints = etcdHosts

	etcdClient, err := clientv3.New(*etcdCfg)
	if err != nil {
		return fmt.Errorf("error creating etcd client: %v", err)
	}
	exec.etcdClient = etcdClient
	defer etcdClient.Close()

	yamlContent, err := os.ReadFile(exec.cfg.YamlPath)
	if err != nil {
		return fmt.Errorf("error reading YAML file: %v", err)
	}

	var configData map[interface{}]interface{}
	if err := yaml.Unmarshal(yamlContent, &configData); err != nil {
		return fmt.Errorf("error unmarshaling YAML content: %v", err)
	}

	parserCfg := &parser.Config{
		TLSRootLevel:      exec.cfg.TLSRootLevel,
		DatabaseRootLevel: exec.cfg.DatabaseRootLevel,
		UpdateConfigKeys:  exec.cfg.UpdateConfigKeys,
		EtcdTLSEnabled:    exec.cfg.EtcdTLSEnabled,
		EtcdHosts:         exec.cfg.EtcdHosts,
		EtcdCaPath:        exec.cfg.EtcdCaPath,
		EtcdCertPath:      exec.cfg.EtcdCertPath,
		EtcdKeyPath:       exec.cfg.EtcdKeyPath,
		EtcdPrefix:        exec.cfg.EtcdPrefix,
		Output:            exec.cfg.Output,
		OutputFilePath:    exec.cfg.OutputFilePath,
	}

	if exec.cfg.Update {
		updateParser := parser.NewUpdateParser(parserCfg, exec.etcdClient)
		if err := updateParser.Parse(configData); err != nil {
			return err
		}
		return nil
	}

	parser := parser.NewParser(parserCfg, exec.etcdClient)
	if err := parser.Parse(configData); err != nil {
		return err
	}
	return nil
}

func createEtcdConfig(cfg *Config) (*clientv3.Config, error) {
	etcdConfig := clientv3.Config{
		DialTimeout: defaultTimeout,
	}

	if cfg.EtcdTLSEnabled {
		certs, err := tls.LoadX509KeyPair(cfg.EtcdCertPath, cfg.EtcdKeyPath)
		if err != nil {
			return nil, fmt.Errorf("unable to load key pair for etcd: %v", err)
		}

		ca, err := os.ReadFile(cfg.EtcdCaPath)
		if err != nil {
			return nil, fmt.Errorf("unabled to load CA for etcd: %v", err)
		}

		caPool := x509.NewCertPool()
		caPool.AppendCertsFromPEM(ca)

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{certs},
			RootCAs:      caPool,
		}

		etcdConfig.TLS = tlsConfig
	}

	return &etcdConfig, nil
}

func validateFlags(config *Config) error {
	if config.YamlPath == "" || config.TLSRootLevel == "" || config.EtcdHosts == "" || config.DatabaseRootLevel == "" {
		return fmt.Errorf("-yaml-file -tls-root-level -database-root-level -etcd-hosts flags are required")
	}

	if config.EtcdTLSEnabled {
		if config.EtcdCaPath == "" || config.EtcdCertPath == "" || config.EtcdKeyPath == "" {
			return fmt.Errorf("either ca path / cert path / key path of the etcd was empty,these flags are required when etcd tls enabled flag was true")
		}
	}

	return nil
}
