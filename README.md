# Producing Earth Science Data for Impact: Improved Commercial Cloud Usability of Archive Model Data

Welcome to the article supplement for McNally et al. (submitted), which introduces concepts, challenges, and solutions for analysis of Earth System Model archive data in cloud computing environments.

## TODO: Supplementary Text Section

## Repository Overview

The [scripts](scripts/) folder provides the code needed to reproduce the case study;
follow the steps described in the folder's README.

The [CONTRIBUTING.md](CONTRIBUTING.md) guide provides instructions for project collaborators,
or any potential contributors to this repository.
It includes documentation on developing and testing in Jupyter notebooks.

Both [scripts/README.md](scripts/README.md) and the contributing guide are executable [MyST Markdown] documents,
with bash as the designated kernel.
If not already available, install this light-weight kernel and kernelspec in your user directory as follows.

```shell
python -m pip install --user bash_kernel
python -m bash_kernel.install
```

With the [Jupytext] extension available in JupyterLab, execution of MyST Markdown code cells is like execution of Notebook code cells.
The difference is that outputs are not saved in a MyST Markdown file.
Open any MyST Markdown as a Notebook (use right-click > "Open With" > "Notebook") to run its code cells.

## Acknowledgments

Please see the associated publication.

[MyST Markdown]: https://mystmd.org/
[Jupytext]: https://jupytext.readthedocs.io/
