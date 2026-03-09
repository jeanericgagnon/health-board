#!/usr/bin/env Rscript
suppressPackageStartupMessages(library(shiny))
shiny::runApp('app.R', host = '127.0.0.1', port = 3838, launch.browser = TRUE)
