# enterprise-plugin

## How to build a plugin

1. Prepare the `pluginpkg` binary

> git clone git@github.com:pingcap/tidb.git
> cd cmd/pluginpkg
> go install
> export PATH=$GOPATH/bin:$PATH

2. Compile the plugin code using `pluginpkg`

> cd $GOPATH/src/github.com/pingcap
> git clone git@github.com:pingcap/enterprise-plugin.git
> cd whitelist
> pluginpkg -pkg-dir . -out-dir .

If everything is fine, you'll get a `whitelist-1.so` in current directory.

## How to use a plugin

Start `tidb-server` with the `plugin-dir` and `plugin-load` arguments to load the  whitelist plugin.

> ./bin/tidb-server -plugin-dir $GOPATH/src/github.com/pingcap/enterprise-plugin/whitelist -plugin-load whitelist-1
