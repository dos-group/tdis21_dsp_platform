# tdis21_dsp_platform

## Helm charts

### Configuration

Charts are deployed using [Helm](https://helm.sh) and located in the `helm-charts` directory. Configure which charts to deploy in the global values.yaml by setting ``enabled: true`` for each desired technology. Cluster sizes and ports for external access can also be specified here.

Each subchart can be deployed by itself and contains its own values.yaml file with futher configurations. If deployed from the umbrella chart, values in the global values.yaml will overwrite the values in the subchart's values.yaml.

### Using the helm charts
Deploy the charts with:
```
helm install [DEPLOYMENT NAME] [CHART / ROOT DIRECTORY] -n [NAMESPACE]
```

Note: when using helm v3, the namespace must already exist.

Uninstall the charts with:
```
helm uninstall [DEPLOYMENT NAME]
```
