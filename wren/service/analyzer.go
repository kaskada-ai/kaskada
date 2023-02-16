package service

import (
	"context"
	"fmt"
	"strings"

	errDetails "google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/kaskada/kaskada-ai/wren/ent"
	"github.com/kaskada/kaskada-ai/wren/ent/schema"
	"github.com/kaskada/kaskada-ai/wren/internal"
)

type ResourceDependency struct {
	resourceType     string
	resourceName     string
	views            []*ent.KaskadaView
	materializations []*ent.Materialization
}

const (
	foreignKeyConstraint = "foreign key constraint"
	numViews             = 3
	numMaterializations  = 3
)

func (r *ResourceDependency) ToErrorDetails() errDetails.PreconditionFailure {
	view_violations := make([]*errDetails.PreconditionFailure_Violation, 0, numViews)
	for _, v := range r.views {
		violation := &errDetails.PreconditionFailure_Violation{
			Type:        foreignKeyConstraint,
			Subject:     fmt.Sprintf("%s: \"%s\" is referenced by view: \"%s\"", r.resourceType, r.resourceName, v.Name),
			Description: fmt.Sprintf("unable to complete operation. the %s: \"%s\" is referenced by view: \"%s\". please delete the view and retry.", r.resourceType, r.resourceName, v.Name),
		}
		view_violations = append(view_violations, violation)
		if numViews == len(view_violations) {
			break
		}
	}
	materialization_violations := make([]*errDetails.PreconditionFailure_Violation, 0, numMaterializations)
	for _, m := range r.materializations {
		violation := &errDetails.PreconditionFailure_Violation{
			Type:        foreignKeyConstraint,
			Subject:     fmt.Sprintf("%s: \"%s\" is referenced by materialization: \"%s\"", r.resourceType, r.resourceName, m.Name),
			Description: fmt.Sprintf("unable to complete operation. the %s: \"%s\" is referenced by materialization: \"%s\". please delete the materialization and retry.", r.resourceType, r.resourceName, m.Name),
		}
		materialization_violations = append(materialization_violations, violation)
		if numMaterializations == len(materialization_violations) {
			break
		}
	}
	return errDetails.PreconditionFailure{
		Violations: append(view_violations, materialization_violations...),
	}
}

type Analyzer interface {
	Analyze(ctx context.Context, owner *ent.Owner, name string, dependencyType schema.DependencyType) (*ResourceDependency, error)
}

type DependencyAnalyzer struct {
	kaskadaViewClient     internal.KaskadaViewClient
	materializationClient internal.MaterializationClient
}

func NewDependencyAnalyzer(kaskadaViewClient *internal.KaskadaViewClient, materializationClient *internal.MaterializationClient) Analyzer {
	return &DependencyAnalyzer{
		kaskadaViewClient:     *kaskadaViewClient,
		materializationClient: *materializationClient,
	}
}

// Analyze implements Analyzer
func (d *DependencyAnalyzer) Analyze(ctx context.Context, owner *ent.Owner, name string, dependencyType schema.DependencyType) (*ResourceDependency, error) {
	views, err := d.kaskadaViewClient.GetKaskadaViewsWithDependency(ctx, owner, name, dependencyType)
	if err != nil {
		return nil, err
	}

	materializaitons, err := d.materializationClient.GetMaterializationsWithDependency(ctx, owner, name, dependencyType)
	if err != nil {
		return nil, err
	}

	resp := ResourceDependency{
		resourceName:     name,
		resourceType:     strings.ToLower(string(dependencyType)),
		views:            views,
		materializations: materializaitons,
	}
	return &resp, nil
}
