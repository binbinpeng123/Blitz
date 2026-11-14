package controller

import (
	"context"
	"fmt"

	"github.com/binbinpeng123/blitz/pkg/dao"
	volcanoapis "volcano.sh/apis/pkg/apis/batch/v1alpha1"
)

func (ctrl *Controller) getVJCount(ns string, phase volcanoapis.JobPhase) int {
	vjs, _ := ctrl.vjInformer.Informer().GetIndexer().ByIndex(vjStatusIndex, fmt.Sprintf("%s/%s", ns, phase))
	return len(vjs)
}

func (ctrl *Controller) createVJ(ctx context.Context, job *dao.JobStatus) (updateFailPhase bool, err error) {
	return
}
