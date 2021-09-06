package main

import (
	"regexp"
	"testing"
)

func Test_check(t *testing.T) {
	tableReg, _ := regexp.Compile("\\w+[23]")
	db, _ := regexp.Compile("sbtest2")
	c := AuditObjects([]AuditItem{
		{DB: db, Table: tableReg},
	})
	if !c.check("root", "sbtest2", "sbtest2", "") {
		t.Fatal("check failed")
	}
}
