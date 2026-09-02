# TaskGrid monitoring

These optional resources target the Prometheus Operator CRDs installed by
kube-prometheus-stack. They are intentionally separate from the core manifests
so TaskGrid can still run on a minimal kind cluster.

    helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
    helm repo update
    helm upgrade --install monitoring prometheus-community/kube-prometheus-stack --namespace monitoring --create-namespace
    kubectl apply -f k8s/monitoring

The ServiceMonitor scrapes /metrics, the PrometheusRule defines availability,
backlog, dead-letter, and error-rate alerts, and the dashboard ConfigMap is
automatically discovered by the chart's Grafana sidecar.
