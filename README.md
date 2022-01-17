# 服务说明

储存服务, 服务有两个 servant:

- raft.StorageServer.raftObj: raft 端口接口
- raft.StorageServer.StorageObj: 业务服务模块

## 在本机测试编译部署

- 本机测试编译

可以在 od-compiler 镜像中编译:

```sh
docker run -it -v/var/run/docker.sock:/var/run/docker.sock -v ~/.kube/config:/root/.kube/config -v`pwd`:/data/src --entrypoint=bash ruanshudong/od-compiler
```

注意: 为了后续发布的需求, 映射了 k8s 的配置进入 docker

- 给 docker 增加 k8s api 域名解析

在 docker 中增加 host:

```sh
echo "172.30.0.7 api.tarsyun.com">> /etc/hosts
```

- build storage 的镜像

```sh
exec-build.sh tarscloud/tars.cppbase cpp build/bin/StorageServer yaml/values.yaml latest true
```

```sh
exec-helm.sh yaml/values.yaml latest
```

- 发布 storageserver 镜像

```sh
exec-deploy.sh tars-dev od-storageserver-v1.0.0.tgz
```

## 在 github 上自动 CI/CD

在 github 上配置了 workflow, 代码提交以后会自动发布到对应的 K8S.因为 K8S 上部署了对应的 github runner(参考 design/CICD).
