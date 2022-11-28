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

# TODO:
# 1. Figure out why the legend ordering is not working properly
# 2. For some results we might want to display packets per second. Figure out
# how to programmatically do that when we want to.
# subset d to be points where `percent_achieved_rate > .95`
args <- commandArgs(trailingOnly=TRUE)
# argument 1: data
d <- read.csv(args[1], sep=",", header = TRUE)
d_postprocess <- read.csv(args[2], sep = ",", header = TRUE)
# argument 3: pdf to write plot into
plot_pdf <- args[3]
# argument 4: metric (p99, or median)
metric <- args[4]
# argument 5: plot type ([full, individual])
plot_type <- args[5]
# argument 6: if individual -- size
# argument 7: if individual -- num_values
d <- d[ which(d$percent_achieved_rate > 0.95),]

# cut out all data where the percentachieved is less than .95
# d <- subset(d, percent_achieved_rate > .95)

options(width=10000)
labels <- c('capnproto' = 'Capnproto', 
            'protobuf' = 'Protobuf', 
            'flatbuffers' = 'Flatbuffers', 
            'redis' = 'Redis',
            'cornflakes1c-dynamic' = 'Cornflakes (Copy)',
            'cornflakes-dynamic' = 'Cornflakes (SG)')

shape_values <- c('capnproto' = 18, 
                  'protobuf' = 8, 
                  'flatbuffers' = 17, 
                  'redis' = 7,
                  'cornflakes1c-dynamic' = 15, 
                  'cornflakes-dynamic' = 19)
color_values <- c('capnproto' = '#e7298a',
                  'protobuf' = '#e6ab02',
                  'flatbuffers' = '#7570b3',
                  'redis' = '#66a61e',
                  'cornflakes1c-dynamic' = '#d95f02',
                  'cornflakes-dynamic' = '#1b9e77')
levels <- c('capnproto', 'protobuf', 'flatbuffers', 'redis', 'cornflakes1c-dynamic', 'cornflakes-dynamic')
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

color_values <- subset_named(color_values, unique_serialization_labels)
shape_values <- subset_named(shape_values, unique_serialization_labels)
labels <- subset_named(labels, unique_serialization_labels)
levels <- subset_flat(levels, unique_serialization_labels)

d$serialization <- factor(d$serialization, levels = levels)
d_postprocess$serialization <- factor(d_postprocess$serialization, levels = levels)
print(levels)

base_plot <- function(data, metric) {
    # data <- subset(data, sdp99 < 300)
    if (metric == "p99") {
        base_plot <- base_p99_plot(data, 100.0)
        base_plot <- label_plot(base_plot)
        return(base_plot)
    } else if (metric == "median") {
        base_plot <- base_median_plot(data, 50.0)
        base_plot <- label_plot(base_plot)
        return(base_plot)
    }
}

base_p99_plot <- function(data, y_cutoff) {
    plot <- ggplot(data,
                    aes(x = maloadgbps,
                        y = mp99,
                        color = serialization,
                        shape = serialization,
                        ymin = mp99 - sdp99,
                        ymax = mp99 + sdp99)) +
            coord_cartesian(ylim=c(0, y_cutoff)) +
    labs(x = "Achieved Load (Gbps)", y = "p99 Latency (µs)")
    return(plot)
}

base_median_plot <- function(data, y_cutoff) {
    plot <- ggplot(data,
                    aes(x = offered_load_gbps,
                        y = avgmedian,
                        color = serialization,
                        shape = serialization,
                        ymin = avgmedian - sdmedian,
                        ymax = avgmedian + sdmedian)) +
            coord_cartesian(ylim=c(0, y_cutoff)) +
    labs(x = "Offered Load (Gbps)", y = "Median Latency (µs)")
    return(plot)
}

label_plot <- function(plot) {
    print("in label plot")
    plot <- plot +
            geom_point(size=2) +
            geom_line(size = 0.5, aes(color=serialization)) +
            scale_shape_manual(values = shape_values, labels = labels, breaks = levels) +
            scale_color_manual(values = color_values ,labels = labels, breaks = levels) +
            scale_fill_manual(values = color_values, labels = labels, breaks=levels) +
            theme_light() +
            scale_x_continuous(n.breaks=8) +
            expand_limits(x = 0, y = 0) +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans"),
                  legend.title = element_blank(),
                  legend.key.size = unit(2, 'mm'),
                  legend.box="vertical",
                  legend.spacing.x = unit(0.1, 'cm'),
                  legend.spacing.y = unit(0.05, 'cm'),
                  legend.text=element_text(size=11),
                  axis.title=element_text(size=11,face="plain", colour="#000000"),
                  axis.text=element_text(size=11, colour="#000000"),
                  legend.title.align=0.5,
                  legend.margin=margin(0,0,0,0),
                    legend.box.margin=margin(-5,-10,-5,-10)) +
            guides(colour=guide_legend(nrow=2, byrow=TRUE),
                   fill=guide_legend(nrow=2, byrow=TRUE),
                   shape=guide_legend(nrow=2, byrow=TRUE))
            

    return(plot)
}

individual_plot <- function(data, metric, size, values) {
    data <- subset(data, num_values == values & total_size == size)
    plot <- base_plot(data, metric)
    print(plot)
    return(plot)
}

full_plot <- function(data, metric) {
    plot <- base_plot(data, metric) +
        facet_grid(total_size ~ num_values, scales="free") +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans"),
                  legend.title = element_blank(),
                  legend.key.size = unit(10, 'mm'),
                  legend.spacing.x = unit(0.1, 'cm'),
                  axis.title=element_text(size=10,face="plain", colour="#000000"),
                  axis.text=element_text(size=8, colour="#000000"))

    print(plot)
    return(plot)
}

tput_plot <- function(data, x_label) {
    y_name <- "maxtputgbps"
    y_label <- "round(maxtputgbps, 1)"
    y_label_height <- "maxtputgbps"
    y_axis <- "Highest Achieved\nLoad (Gbps)"
    # TODO: for size plot, x should be reordered by size
    plot <- ggplot(data,
                    aes(x =reorder(factor_name, num_values),
                        y=maxtputgbps,
                        fill = serialization)) +
                   expand_limits(y = 0) +
            geom_point(size = 3, stroke=0.2, position=position_dodge(0.8), stat="identity", aes(color=serialization, shape=serialization,fill=serialization, size=serialization)) +
            geom_bar(position=position_dodge(0.8), stat="identity", width = 0.05) +
            guides(colour=guide_legend(nrow=2, byrow=TRUE),
                   shape=guide_legend(nrow=2, byrow=TRUE)) +
          geom_text(position = position_dodge(0.8),
                    aes(y=maxtputgbps + 9, label = round(maxtputgbps, 1)),
                   size = 2.75,
                   angle = 70) +
            scale_color_manual(values = color_values ,labels = labels, breaks=levels) +
            scale_fill_manual(values = color_values, labels = labels, guide = "none", breaks=levels) +
            scale_shape_manual(values = shape_values, labels = labels, breaks=levels) +
            scale_y_continuous(expand = expansion(mult = c(0, .2))) +
            labs(x = x_label, y = y_axis) +
            theme_light() +
            scale_x_discrete(labels = function(x) str_wrap(x, width = 6)) +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans"),
                  legend.title = element_blank(),
                  legend.key.size = unit(2, 'mm'),
                  legend.spacing.x = unit(0.1, 'cm'),
                  legend.spacing.y = unit(0.05, 'cm'),
                  legend.text=element_text(size=11),
                  axis.title=element_text(size=11,face="plain", colour="#000000"),
                  axis.text.y=element_text(size=11, colour="#000000"),
                  axis.text.x=element_text(size=8, colour="#000000", angle=0),
                  legend.margin=margin(0,0,0,0),
                  legend.box.margin=margin(-5,-10,-5,-10))
    print(plot)
    return(plot)
}


d$total_size = d$avg_size * d$num_values;
summarized <- ddply(d, c("serialization", "total_size", "avg_size", "num_values", "offered_load_pps", "offered_load_gbps"), summarise,
                        mavg = mean(avg),
                        mp99 = mean(p99),
                        avgmedian = mean(median),
                        mp999 = mean(p999),
                        sdp99 = sd(p99),
                        sdmedian = sd(median),
                        mprate = mean(percent_achieved_rate),
                        maloadgbps = mean(achieved_load_gbps),
                        maload = mean(achieved_load_pps))

# cutoff points where the sd of the p99 is > 50 % of the p99?
# summarized$sdp99_percent = summarized$sdp99 / summarized$mp99
# summarized <- subset(summarized, summarized$sdp99_percent < .25)



if (plot_type == "full") {
    plot <- full_plot(summarized, metric)
    ggsave("tmp.pdf", width=9, height=9)
    embed_fonts("tmp.pdf", outfile=plot_pdf)
} else if (plot_type == "individual") {
    total_size <- strtoi(args[6])
    num_values <- strtoi(args[7])
    plot <- individual_plot(summarized, metric, total_size, num_values)
    ggsave("tmp.pdf", width=5, height=2)
    embed_fonts("tmp.pdf", outfile=plot_pdf)
} else if (plot_type == "summary") {
    size_arg <- strtoi(args[6])
    d_postprocess <- subset(d_postprocess, total_size == size_arg)
    plot <- tput_plot(d_postprocess, args[7])
    print(plot_pdf)
    ggsave("tmp.pdf", width=5, height=2)
    embed_fonts("tmp.pdf", outfile=plot_pdf)
} else if (plot_type == "summary_num_values") {
    num_values_arg <- strtoi(args[6])
    d_postprocess <- subset(d_postprocess, num_values == num_values)
    plot <- tput_plot(d_postprocess, args[7])
    print(plot_pdf)
    ggsave("tmp.pdf", width=5, height=2)
    embed_fonts("tmp.pdf", outfile=plot_pdf)

}

