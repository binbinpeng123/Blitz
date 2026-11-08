package conf

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// LoadFromFile 读取本地 YAML 配置并填充全局 Conf。
// 用于不依赖 Nacos 的部署场景；调用方负责后续 Conf.Check()。
func LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %s: %w", path, err)
	}
	if err = yaml.Unmarshal(data, &Conf); err != nil {
		return fmt.Errorf("unmarshal config %s: %w", path, err)
	}
	
	return nil
}
