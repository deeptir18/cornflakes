#!/bin/bash

sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
sudo add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu focal-cran40/'
sudo apt update -y
sudo apt install r-base -y
sudo -i R

Rscript -e 'install.packages("ggplot2", repos="https://cloud.r-project.org")'
Rscript -e 'install.packages("tidyr", repos="https://cloud.r-project.org")'
Rscript -e 'install.packages("extrafont", repos="https://cloud.r-project.org")'
Rscript -e 'install.packages("showtext", repos="https://cloud.r-project.org")'
Rscript -e 'install.packages("viridis", repos="https://cloud.r-project.org")'


