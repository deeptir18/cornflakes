#!/bin/bash

sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
sudo add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu focal-cran40/'
sudo apt update -y
sudo apt install r-base -y
sudo apt-get install -y libcurl4-openssl-dev
sudo apt-get install -y ghostscript

sudo R -e 'install.packages(c("plyr", "tidyr", "ggplot2", "extrafont",
"sysfonts", "showtext", "curl", "json-lite", "viridis"))'


