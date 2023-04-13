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

min_num_keys <- args[5]
# argument 6: size to subset by
size_subset <- args[6]
# argument 7: whether the run ignored sets or not
ignore_sets_subset <- args[7]
# argument 8: which distribution the run used (exponential or uniform)
distribution_subset <- args[8]


# size arg (if non 0 -> graph size subset)
size_subset_metric <- args[9]
size_subset_pps <- args[10]

d <- d[ which(d$min_num_keys == min_num_keys),]
d <- d[ which(d$value_size == size_subset),]
d <- d[ which(d$ignore_sets == ignore_sets_subset),]
d <- d[ which(d$distribution == distribution_subset),]
d <- d[ which(d$percent_achieved_rate > 0.95),]

options(width=10000)
cr_labels_baselines <- c('capnproto' = 'CP', 
            'protobuf' = 'PB', 
            'flatbuffers' = 'FB', 
            'redis' = 'Redis',
            'cornflakes-dynamic-512' = 'CF')
anon_labels_baselines <- c('capnproto' = 'CP', 
            'protobuf' = 'PB', 
            'flatbuffers' = 'FB', 
            'redis' = 'Redis',
            'cornflakes-dynamic-512' = 'AS')
                  
cr_labels_cf <- c('cornflakes1c-dynamic' = 'Only Copy',
                  'cornflakes-dynamic-512' = 'Thresh = 512',
                  'cornflakes-dynamic-256' = 'Thresh = 256',
                  'cornflakes-dynamic-0' = 'Only SG'
                  )
anon_labels_cf <- c(
                   'cornflakes1c-dynamic' = 'Only Copy',
                  'cornflakes-dynamic-512' = 'Thresh = 512',
                  'cornflakes-dynamic-256' = 'Thresh = 256',
                  'cornflakes-dynamic-0' = 'Only SG'
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
                  'cornflakes1c-dynamic' = '#a6cee3',
                  'cornflakes-dynamic-0' = '#1f78b4',
                  'cornflakes-dynamic-256' = '#b2df8a',
                  'cornflakes-dynamic-512' = '#33a02c')    
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
        base_plot <- base_pps_p99_plot(data, 100.0)
        base_plot <- label_plot(base_plot, labels, shape_values, color_values, specific_levels)
        return(base_plot)
    } else if (metric == "median") {
        base_plot <- base_pps_median_plot(data, 50.0)
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
                    aes(x = !!p99_str,
                        y = !!load_str / 1000,
                        color = serialization,
                        shape = serialization)) +
            coord_cartesian(xlim=c(0, x_cutoff)) +
    labs(y = "Achieved Load\n(1000 Packets Per Second)", x = "p99 latency (µs)")
    return(plot)
}
base_pps_median_plot <- function(data, x_cutoff) {
    load_str = sym("achieved_load_pps")
    median_str = sym("median")
    if (!is.na(size_subset_metric)) {
        median_str = sym(size_subset_metric)
    }

    if (!is.na(size_subset_pps)) {
        load_str = sym(size_subset_pps)
    }

    plot <- ggplot(data,
                    aes(x = !!median_str,
                        y = !!load_str / 1000,
                        color = serialization,
                        shape = serialization)) +
            coord_cartesian(xlim=c(0, x_cutoff)) +
    labs(y = "Achieved Load\n(1000 Packets Per Second)", x = "Median Latency (µs)")
    return(plot)
}

label_plot <- function(plot, labels, shape_values, color_values, specific_levels) {
    plot <- plot +
            geom_point(size=1.5) +
            geom_line(linewidth = 0.5, aes(color=serialization), orientation = "y") +
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
            guides(colour=guide_legend(nrow=1, byrow=TRUE, override.aes = list(size = 3)),
                   fill=guide_legend(nrow=1, byrow=TRUE),
                   shape=guide_legend(nrow=1, byrow=TRUE))
    return(plot)
}

if (plot_type == "baselines") {
    d <- subset(d, d$serialization %in% levels_baselines)
    d$serialization <- factor(d$serialization, levels = levels_baselines)
    plot <- base_plot(d, metric, cr_labels_baselines, shape_values_baselines, color_values_baselines, levels_baselines)
    ggsave("tmp.pdf", width = 6, height = 3)
    embed_fonts("tmp.pdf", outfile =  cr_plot_pdf)
    
    plot <- base_plot(d, metric, anon_labels_baselines, shape_values_baselines, color_values_baselines, levels_baselines)
    ggsave("tmp.pdf", width = 6, height = 3)
    embed_fonts("tmp.pdf", outfile =  anon_plot_pdf)
} else if (plot_type == "cornflakes") {
    d <- subset(d, d$serialization %in% levels_cf)
    d$serialization <- factor(d$serialization, levels = levels_cf)
    plot <- base_plot(d, metric, cr_labels_cf, shape_values_cf, color_values_cf, levels_cf)
    ggsave("tmp.pdf", width = 6, height = 3)
    embed_fonts("tmp.pdf", outfile =  cr_plot_pdf)
    
    plot <- base_plot(d, metric, anon_labels_cf, shape_values_cf, color_values_cf, levels_cf)
    ggsave("tmp.pdf", width = 6, height = 3)
    embed_fonts("tmp.pdf", outfile =  anon_plot_pdf)
}
