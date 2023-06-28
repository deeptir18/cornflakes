#!/bin/bash

# This script contains ubuntu packages necessary to install graphing utilities
# for cornflakes experiments.

sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
sudo add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu focal-cran40/'
sudo apt update -y
sudo apt install r-base=4.1.2-1.2004.0
sudo apt install r-base-dev=4.1.2-1.2004.0
sudo apt-get install -y libcurl4-openssl-dev ghostscript
sudo apt-get install -y libfreetype6-dev libpng-dev libtiff5-dev libjpeg-dev
sudo apt-get install libfontconfig1-dev libudunits2-dev libxml2-dev
sudo apt-get install libharfbuzz-dev libfribidi-dev
sudo apt install libgeos-dev libgdal-dev

Rscript install-R-packages.R
