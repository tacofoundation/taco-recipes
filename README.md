<div align="center">
<img src="https://data.source.coop/taco/logo/taco_logo.png" height="200"/>

# taco-recipes

**Source code for published TACO datasets**

</div>

---

## What is this?

This repository contains the scripts used to create TACO datasets. Each folder is a [cookiecutter-taco](https://github.com/tacofoundation/cookiecutter-taco) project. If you want to reproduce, adapt, or understand how a dataset was built, you're in the right place.

## Datasets

| Dataset | Description |
|---------|-------------|
| [3dclouds](./3dclouds/) | Global 3D cloud reconstruction from geostationary satellites + CloudSat |


## Quick Start

```bash
# Create a new dataset from the template
pip install cookiecutter
cookiecutter https://github.com/tacofoundation/cookiecutter-taco
```

## Related

- [cookiecutter-taco](https://github.com/tacofoundation/cookiecutter-taco) - Template for creating TACO datasets
- [TacoToolbox](https://github.com/tacofoundation/taco-toolbox) - Create TACO datasets
- [TacoReader](https://github.com/tacofoundation/tacoreader) - Query TACO datasets

## License

MIT