package controller

import (
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	volcanoapis "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	volcanoinformerv1 "volcano.sh/apis/pkg/client/informers/externalversions/batch/v1alpha1"
)

// fakeSharedIndexInformer 只覆写 GetIndexer，其余方法由内嵌接口兜底。
type fakeSharedIndexInformer struct {
	cache.SharedIndexInformer
	indexer cache.Indexer
}

func (f *fakeSharedIndexInformer) GetIndexer() cache.Indexer {
	return f.indexer
}

type fakeVJInformer struct {
	volcanoinformerv1.JobInformer
	informer cache.SharedIndexInformer
}

func (f *fakeVJInformer) Informer() cache.SharedIndexInformer {
	return f.informer
}

// newFakeVJInformer 返回带 vjStatusIndex 索引的假 informer，索引 key 与 getVJCount 保持一致。
func newFakeVJInformer(t *testing.T) (volcanoinformerv1.JobInformer, cache.Indexer) {
	t.Helper()
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		vjStatusIndex: func(obj interface{}) ([]string, error) {
			job, ok := obj.(*volcanoapis.Job)
			if !ok {
				return nil, fmt.Errorf("unexpected object type %T", obj)
			}
			return []string{fmt.Sprintf("%s/%s", job.Namespace, job.Status.State.Phase)}, nil
		},
	})
	return &fakeVJInformer{informer: &fakeSharedIndexInformer{indexer: idx}}, idx
}

func testVJ(ns, name string, phase volcanoapis.JobPhase) *volcanoapis.Job {
	return &volcanoapis.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Status: volcanoapis.JobStatus{
			State: volcanoapis.JobState{Phase: phase},
		},
	}
}

func TestGetVJCount(t *testing.T) {
	informer, idx := newFakeVJInformer(t)
	for _, job := range []*volcanoapis.Job{
		testVJ("default", "vj-1", volcanoapis.Pending),
		testVJ("default", "vj-2", volcanoapis.Pending),
		testVJ("default", "vj-3", volcanoapis.Running),
		testVJ("other", "vj-4", volcanoapis.Pending),
		testVJ("default", "vj-5", ""),
	} {
		if err := idx.Add(job); err != nil {
			t.Fatalf("indexer.Add(%s): %v", job.Name, err)
		}
	}

	ctrl := &Controller{vjInformer: informer}

	if got := ctrl.getVJCount("default", volcanoapis.Pending); got != 2 {
		t.Fatalf("default/Pending = %d, want 2", got)
	}
	if got := ctrl.getVJCount("default", volcanoapis.Running); got != 1 {
		t.Fatalf("default/Running = %d, want 1", got)
	}
	if got := ctrl.getVJCount("default", volcanoapis.Completed); got != 0 {
		t.Fatalf("default/Completed = %d, want 0", got)
	}
	if got := ctrl.getVJCount("other", volcanoapis.Pending); got != 1 {
		t.Fatalf("other/Pending = %d, want 1", got)
	}
}
