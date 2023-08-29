# Installation

To install Kaskada, you need to be using Python >= 3.8.
We suggest using 3.11 or newer, since that provides more precise error locations.

```{code-block} bash
:caption: Installing Kaskada
pip install kaskada>=0.6.0-a.0
```

```{warning}
This version of Kaskada is currently a pre-release, as indicated by the `-a.0` suffix.
It will not be installed by default if you `pip install kaskada`.
You need to either use `pip install --pre kaskada` or specify a specific version, as shown in the example.
```

```{tip}
Depending on you Python installation and configuration you may have `pip3` instead of `pip` available in your terminal.
If you do have `pip3` replace pip with `pip3` in your command, i.e., `pip3 install kaskada`.

If you get a permission error when running the `pip` command, you may need to run as an administrator using `sudo pip install kaskada`.
If you don't have administrator access (e.g., in Google Colab, or other hosted environments) you amy use `pip`’s `--user` flag to install the package in your user directory.
```
