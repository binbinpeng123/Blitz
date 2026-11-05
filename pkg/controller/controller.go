package controller

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/binbinpeng123/blitz/pkg/dao"
	informerv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
)

type Controller struct {
	db                     *dao.Dao
	submitingNSsWithCancel map[string]context.CancelFunc
	k8s                    string

	// informerSynced 标记 informer initial list 是否完成。用于区分 AddFunc 触发来源：
	// false → 初次 list（reconcile metric trigger=startup）；true → 后续 watch 事件。
	informerSynced atomic.Bool

	// k8s
	kubeClient  kubernetes.Interface
	podQueue    workqueue.RateLimitingInterface
	podInformer informerv1.PodInformer
	nsListener  listersv1.NamespaceLister
}

func NewController(config *rest.Config, kubeClient kubernetes.Interface, k8s string) (*Controller, error) {
	// 获取数据库实例
	db := dao.Get(k8s)
	if db == nil {
		return nil, fmt.Errorf("database not initialized for cluster %s", k8s)
	}

	ctrl := &Controller{
		db:         db,
		kubeClient: kubeClient,
		k8s:        k8s,
		podQueue:   workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "pods"),
	}

	return ctrl, nil
}

func (ctrl *Controller) Run(ctx context.Context, workers int) {

}
