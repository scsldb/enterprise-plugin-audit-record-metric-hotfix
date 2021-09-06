module github.com/pingcap/enterprise-plugin/whitelist

require (
	github.com/pingcap/log v0.0.0-20200511115504-543df19646ad
	github.com/pingcap/tidb v2.0.11+incompatible
	go.uber.org/zap v1.15.0
)

replace github.com/pingcap/tidb => ../../tidb

go 1.13

replace github.com/pingcap/pd/v4 => github.com/nolouch/pd/v4 v4.0.0-20210831114947-686590ed34cd
