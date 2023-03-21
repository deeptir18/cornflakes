#!/usr/bin/env Rscript

library(ggplot2)
library(plyr)
library(tidyr)
library(extrafont)
library(showtext)
library(viridis)
library("stringr")  
font_add_google("Fira Sans")
showtext_auto()

args <- commandArgs(trailingOnly=TRUE)
# argument 1: data
d <- read.csv(args[1], sep=",", header = TRUE)
# argument 2: pdf to write plot into
plot_pdf <- args[2]
basename <- sub('\\.pdf$', '', plot_pdf) 
cr_plot_pdf <- paste(basename, "cr.pdf", sep = "_")
anon_plot_pdf <- paste(basename, "anon.pdf", sep = "_")

# argument 3: metric (p99, or median)
metric <- args[3]
# argument 4: either "baselines" or "cornflakes" (to compare copy thresholds)
plot_type <- args[4]

# size arg (if non 0 -> graph size subset)
size_subset_metric <- args[5]
size_subset_pps <- args[6]

# cut out data where percent achieved is less than 0.95
# d <- d[ which(d$percent_achieved_rate > 0.95),]

options(width=10000)
cr_labels_baselines <- c('capnproto' = 'Capnproto', 
            'protobuf' = 'Protobuf', 
            'flatbuffers' = 'Flatbuffers', 
            'redis' = 'Redis',
            'cornflakes-dynamic-512' = 'Cornflakes (thresh = 512)')
anon_labels_baselines <- c('capnproto' = 'Capnproto', 
            'protobuf' = 'Protobuf', 
            'flatbuffers' = 'Flatbuffers', 
            'redis' = 'Redis',
            'cornflakes-dynamic-512' = 'AnonSys (thresh = 512))')
                  
cr_labels_cf <- c('cornflakes1c-dynamic' = 'Cornflakes (only copy)',
                  'cornflakes-dynamic-512' = 'Cornflakes (thresh = 512)',
                  'cornflakes-dynamic-256' = 'Cornflakes (thresh = 256)',
                  'cornflakes-dynamic-0' = 'Cornflakes (only SG)'
                  )
anon_labels_cf <- c(
                   'cornflakes1c-dynamic' = 'AnonSys (only copy)',
                  'cornflakes-dynamic-512' = 'AnonSys (thresh = 512)',
                  'cornflakes-dynamic-256' = 'AnonSys (thresh = 256)',
                  'cornflakes-dynamic-0' = 'AnonSys (only SG)'
                  )
                  
shape_values_baselines <- c('capnproto' = 18, 
                  'protobuf' = 8, 
                  'flatbuffers' = 17, 
                  'redis' = 7,
                  'cornflakes-dynamic-512' = 19)
color_values_baselines <- c(
                  'capnproto' = '#e7298a',
                  'protobuf' = '#e6ab02',
                  'flatbuffers' = '#7570b3',
                  'redis' = '#66a61e',
                  'cornflakes-dynamic-512' = '#1b9e77')

shape_values_cf <- c(
                  'cornflakes1c-dynamic' = 19,
                  'cornflakes-dynamic-0' = 19,
                  'cornflakes-dynamic-256' = 19,
                  'cornflakes-dynamic-512' = 19)
color_values_cf <- c(
                  'cornflakes1c-dynamic' = '#cccccc',
                  'cornflakes-dynamic-0' = '#969696',
                  'cornflakes-dynamic-256' = '#636363',
                  'cornflakes-dynamic-512' = '#252525')    
levels_baselines <- c('cornflakes-dynamic-512', 'redis', 'flatbuffers', 'protobuf', 'capnproto')
levels_cf <- c('cornflakes-dynamic-512', 'cornflakes-dynamic-256', 'cornflakes-dynamic-0', 'cornflakes1c-dynamic')
# filter the serialization labels based on which are present in data
unique_serialization_labels <- unique(c(d$serialization))
subset_flat <- function(original, subset) {
    x <- c()
    for (name in original) {
        if (name %in% subset) {
            x <- append(x, name)
        }
    }
    return(x)
}

subset_named <- function(original, subset) {
    x <- c()
    attr_name <- attributes(original)$name
    attrs <- c()
    for (name in attr_name) {
        if (name %in% subset) {
            x <- append(x, original[name])
            attrs <- append(attrs, name)
        }
    }
    names(x) <- attrs
    return(x)
}

color_values_baselines <- subset_named(color_values_baselines, unique_serialization_labels)
shape_values_baselines <- subset_named(shape_values_baselines, unique_serialization_labels)
color_values_cf <- subset_named(color_values_cf, unique_serialization_labels)
shape_values_cf <- subset_named(shape_values_cf, unique_serialization_labels)
cr_labels_baselines <- subset_named(cr_labels_baselines, unique_serialization_labels)
anon_labels_baselines <- subset_named(anon_labels_baselines, unique_serialization_labels)
cr_labels_cf <- subset_named(cr_labels_cf, unique_serialization_labels)
anon_labels_cf <- subset_named(anon_labels_cf, unique_serialization_labels)
levels_baselines <- subset_flat(levels_baselines, unique_serialization_labels)
levels_cf <- subset_flat(levels_cf, unique_serialization_labels)



base_plot <- function(data, metric, labels, shape_values, color_values, specific_levels) {
    if (metric == "p99") {
        base_plot <- base_pps_p99_plot(data, 500.0)
        base_plot <- label_plot(base_plot, labels, shape_values, color_values, specific_levels)
        return(base_plot)
    } else if (metric == "median") {
        base_plot <- base_pps_median_plot(data, 500.0)
        base_plot <- label_plot(base_plot, labels, shape_values, color_values, specific_levels)
        return(base_plot)
    }
}

base_pps_p99_plot <- function(data, x_cutoff) {
    load_str = sym("achieved_load_pps")
    p99_str = sym("p99")
    if (!is.na(size_subset_metric)) {
        p99_str = sym(size_subset_metric)
    }

    if (!is.na(size_subset_pps)) {
        load_str = sym(size_subset_pps)
    }

    plot <- ggplot(data,
                    aes(y = !!p99_str,
                        x = !!load_str,
                        color = serialization,
                        shape = serialization)) +
            # coord_cartesian(xlim=c(0, x_cutoff)) +
    labs(x = "Achieved Load\n(Packets Per Second)", y = "p99 latency (µs)")
    return(plot)
}
base_pps_median_plot <- function(data, x_cutoff) {
    data$achieved_load_pps <- data$achieved_load_pps/1000.0
    load_str = sym("achieved_load_pps")
    median_str = sym("median")
    if (!is.na(size_subset_metric)) {
        median_str = sym(size_subset_metric)
    }

    if (!is.na(size_subset_pps)) {
        load_str = sym(size_subset_pps)
    }

    plot <- ggplot(data,
                    aes(y = !!median_str,
                        x = !!load_str,
                        color = serialization,
                        shape = serialization)) +
            # coord_cartesian(xlim=c(0, x_cutoff)) +
    labs(x = "Achieved Load\n(Packets Per Second)", y = "Median Latency (µs)")
    return(plot)
}

label_plot <- function(plot, labels, shape_values, color_values, specific_levels) {
    plot <- plot +
            geom_point(size=1.75) +
            geom_line(linewidth = 0.5, aes(color=serialization), orientation = "x") +
            scale_shape_manual(values = shape_values, labels = labels, breaks = specific_levels) +
            scale_color_manual(values = color_values ,labels = labels, breaks = specific_levels) +
            scale_fill_manual(values = color_values, labels = labels, breaks= specific_levels) +
            theme_light() +
            scale_y_continuous(n.breaks=8) +
            expand_limits(x = 0, y = 0) +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans"),
                  legend.title = element_blank(),
                  legend.key.size = unit(2, 'mm'),
                  legend.box="vertical",
                  legend.spacing.x = unit(0.5, 'cm'),
                  legend.spacing.y = unit(0.05, 'cm'),
                  legend.text=element_text(size=15),
                  axis.title=element_text(size=15,face="plain", colour="#000000"),
                  axis.text=element_text(size=15, colour="#000000"),
                  legend.title.align=0.5,
                  legend.margin=margin(0,0,0,0),
                    legend.box.margin=margin(-5,-10,-5,-10)) +
            guides(colour=guide_legend(nrow=2, byrow=TRUE, override.aes = list(size = 3)),
                   fill=guide_legend(nrow=2, byrow=TRUE),
                   shape=guide_legend(nrow=2, byrow=TRUE))
    return(plot)
}

if (plot_type == "baselines") {
    d$serialization <- factor(d$serialization, levels = levels_baselines)
    plot <- base_plot(d, metric, cr_labels_baselines, shape_values_baselines, color_values_baselines, levels_baselines)
    ggsave("tmp.pdf", width = 9, height = 9)
    embed_fonts("tmp.pdf", outfile =  cr_plot_pdf)
    
    plot <- base_plot(d, metric, anon_labels_baselines, shape_values_baselines, color_values_baselines, levels_baselines)
    ggsave("tmp.pdf", width = 9, height = 9)
    embed_fonts("tmp.pdf", outfile =  anon_plot_pdf)
} else if (plot_type == "cornflakes") {
    d$serialization <- factor(d$serialization, levels = levels_cf)
    plot <- base_plot(d, metric, cr_labels_cf, shape_values_cf, color_values_cf, levels_cf)
    ggsave("tmp.pdf", width = 9, height = 9)
    embed_fonts("tmp.pdf", outfile =  cr_plot_pdf)
    
    plot <- base_plot(d, metric, anon_labels_cf, shape_values_cf, color_values_cf, levels_cf)
    ggsave("tmp.pdf", width = 9, height = 9)
    embed_fonts("tmp.pdf", outfile =  anon_plot_pdf)
}
