package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	cmdutil "github.com/argoproj/argo-workflows/v3/util/cmd"
	"github.com/argoproj/argo-workflows/v3/util/logs"
	kubecli "github.com/argoproj/pkg/kube/cli"
	"github.com/argoproj/pkg/stats"
	"github.com/binbinpeng123/blitz/pkg/conf"
	"github.com/binbinpeng123/blitz/pkg/controller"
	"github.com/binbinpeng123/blitz/pkg/dao"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	runtimeutil "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
)

const (
	CliName = "blitz-controller"
)

func main() {
	if err := newRootCommand().ExecuteContext(context.Background()); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	opts := controller.DefaultOptions()
	cmd := &cobra.Command{
		Use:           CliName,
		Short:         "Blitz training job controller",
		Args:          cobra.NoArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd.Context(), opts)
		},
	}

	// kubecli 已注册 --kubeconfig/--context 等 flag，此处不要重复注册，否则 cobra 会 panic。
	opts.ClientConfig = kubecli.AddKubectlFlagsToCmd(cmd)
	cmd.Flags().StringVar(&opts.ConfigFile, "config", opts.ConfigFile, "path to blitz config file")
	cmd.Flags().StringVar(&opts.K8s, "k8s", opts.K8s, "cluster name, must be a key of k8s_kubeconfig/db_dsn")
	cmd.Flags().IntVar(&opts.Workers, "workers", opts.Workers, "number of reconcile workers")
	cmd.Flags().DurationVar(&opts.Resync, "resync", opts.Resync, "active job scan interval")
	cmd.Flags().StringVar(&opts.LogLevel, "loglevel", "info", "Set the logging level. One of: debug|info|warn|error")
	cmd.Flags().IntVar(&opts.GLogLevel, "gloglevel", 0, "Set the glog logging level")
	cmd.Flags().StringVar(&opts.LogFormat, "log-format", "text", "The formatter to use for logs. One of: text|json")

	return cmd
}

func run(ctx context.Context, opts *controller.Options) error {
	defer runtimeutil.HandleCrash()
	if err := opts.Validate(); err != nil {
		return err
	}

	// 1.设置日志级别和格式、打印日志
	cmdutil.SetGLogLevel(opts.GLogLevel)
	cmdutil.SetLogFormatter(opts.LogFormat)
	level, err := log.ParseLevel(opts.LogLevel)
	if err != nil {
		return fmt.Errorf("invalid log level %q: %w", opts.LogLevel, err)
	}
	log.SetLevel(level)
	stats.RegisterStackDumper()
	stats.StartStatsTicker(5 * time.Minute)

	// 2.加载业务配置
	if opts.ConfigFile == "" {
		return fmt.Errorf("--config must be set")
	}
	if err := conf.Load(opts.ConfigFile); err != nil {
		return err
	}

	// 3.设置 k8s 客户端配置
	config, err := opts.ClientConfig.ClientConfig()
	if err != nil {
		return err
	}
	config = restclient.AddUserAgent(config, "infra-computing/blitz-controller")
	config.Burst = opts.Burst
	config.QPS = opts.Qps

	// 4.Kubernetes内置资源默认走PB协议（主要是POD），避免informer的序列化瓶颈
	// 该配置仅APIServer返回内容格式优先PB，POST/PUT等请求的body依旧为JSON
	// 对于VolcanoJob、Workflow这些CRD资源，APIServer默认不支持PB，因此依旧会走JSON协议
	if os.Getenv("INFORMER_PROTOBUF_DISABLED") != "true" {
		config.AcceptContentTypes = "application/vnd.kubernetes.protobuf,application/json"
	}

	// 5.日志、打点
	logs.AddK8SLogTransportWrapper(config)

	// 6.创建k8s客户端
	kubeClient := kubernetes.NewForConfigOrDie(config)

	// 7.创建生命周期context
	signalCtx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	ctx, cancel := context.WithCancel(signalCtx)
	defer cancel()

	// 8.初始化DB
	if opts.K8s == "" {
		return fmt.Errorf("k8s name must be set")
	}
	if err := dao.InitDB(ctx); err != nil {
		return fmt.Errorf("init database failed: %w", err)
	}
	defer func() {
		if err := dao.CloseDB(); err != nil {
			log.Errorf("close database failed: %v", err)
		}
	}()

	// 9.创建Controller
	ctrl, err := controller.NewController(config, kubeClient, opts.K8s)
	if err != nil {
		return fmt.Errorf("new controller failed: %w", err)
	}

	// 10.选主并启动Controller
	// 注意：选主逻辑尚未实现，未关闭选主时直接返回错误，避免进程静默空转。
	if os.Getenv("LEADER_ELECTION_DISABLE") != "true" {
		return fmt.Errorf("leader election is not implemented yet, set LEADER_ELECTION_DISABLE=true to run in single-instance mode")
	}
	log.Info("Leader election is turned off. Running in single-instance mode")
	log.WithField("id", "single-instance").Info("starting leading")
	go ctrl.Run(ctx, opts.Workers)
	<-ctx.Done()

	return nil
}
