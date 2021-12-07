#!/usr/bin/env Rscript

library(ggplot2)
library(plyr)
library(tidyr)
library(extrafont)
library(showtext)
library(viridis)
font_add_google("Fira Sans")
showtext_auto()

args <- commandArgs(trailingOnly=TRUE)
d <- read.csv(args[1], sep=",", header = TRUE)
# argument 2: pdf to write plot into
plot_pdf <- args[2]
# argument 3: metric (p99, or median)
metric <- args[3]
# argument 4: plot type ([full, individual])
plot_type <- args[4]
# argument 5: if individual -- message type
# argument 6: if individual -- size (of message total)

labels <- c("protobuf" = "Protobuf", "capnproto" = "Capnproto", "flatbuffers" = "Flatbuffers", "cereal" = "Cereal", "cornflakes-dynamic" = "Cornflakes (Hardware SG)", "cornflakes1c-dynamic" = "Cornflakes (1 Software Copy)", "ideal" = "Peak Single-Core", "onecopy" = "One Copy", "twocopy" = "Two Copies")

shape_values <- c('protobuf' = 7, 'cereal' = 4, 'capnproto' = 18, 'flatbuffers' = 17, 'cornflakes1c-dynamic' = 15, 'cornflakes-dynamic' = 19, "ideal" = 20, "onecopy" = 1, "twocopy" = 10)

color_values <- c('cornflakes-dynamic' = '#1b9e77', 
                    'cornflakes1c-dynamic' = '#d95f02',
                    "ideal" = '#333333',
                    "onecopy" = '#666666',
                    "twocopy" = "#999999",
                    'flatbuffers' = '#7570b3',
                    'capnproto' = '#e7298a',
                    'cereal' = '#66a61e',
                    'protobuf' = '#e6ab02')
options(width=10000)

base_plot <- function(data, metric) {
    if (metric == "p99") {
        base_plot <- base_p99_plot(data, 100.0)
        base_plot <- label_plot(base_plot)
    } else if (metric == "median") {
        base_plot <- base_median_plot(data, 50.0)
        base_plot <- label_plot(base_plot)
    }
}

base_p99_plot <- function(data, y_cutoff) {
    plot <- ggplot(data,
                    aes(x = offered_load_gbps,
                        y = mp99,
                        color = serialization,
                        shape = serialization,
                        ymin = mp99 - sdp99,
                        ymax = mp99 + sdp99)) +
            coord_cartesian(ylim=c(0, y_cutoff), xlim=c(0, 100), expand = FALSE) +
    labs(x = "Offered Load (Gbps)", y = "p99 Latency (µs)")
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
            coord_cartesian(ylim=c(0, y_cutoff), expand = FALSE) +
    labs(x = "Offered Load (Gbps)", y = "Median Latency (µs)")
    return(plot)
}

label_plot <- function(plot) {
    plot <- plot +
            geom_point(size=5) +
            geom_line(size = 1, aes(color=serialization)) +
            scale_shape_manual(values = shape_values, labels = labels) +
            scale_color_manual(values = color_values ,labels = labels) +
            scale_fill_manual(values = color_values, labels = labels) +
            theme_light() +
            expand_limits(x = 0, y = 0) +
            scale_x_continuous(n.breaks=5) +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans"),
                  legend.title = element_blank(),
                  legend.key.size = unit(10, 'mm'),
                  legend.spacing.x = unit(0.1, 'cm'),
                   legend.text=element_text(size=18),
                  axis.title=element_text(size=27,face="plain", colour="#000000"),
                  axis.text=element_text(size=27, colour="#000000"))
    return(plot)
}

individual_plot <- function(data, metric, total_size, msg_type) {
    data <- subset(data, message_type == msg_type & size == total_size)
    plot <- base_plot(data, metric)
    print(plot)
    return(plot)
}

full_plot <- function(data, metric) {
    plot <- base_plot(data, metric) +
        facet_grid(message_type ~ size, scales="free") +
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
summarized <- ddply(d, c("serialization", "size", "message_type", "offered_load_pps", "offered_load_gbps"),
                    summarise,
                    mavg = mean(avg),
                    mp99 = mean(p99),
                    mp999 = mean(p999),
                    avgmedian = mean(median),
                    sdp99 = sd(p99),
                    sdmedian = sd(median),
                    maloadgbps = mean(achieved_load_gbps),
                    maload = mean(achieved_load_pps))

if (plot_type == "full") {
    plot <- full_plot(summarized, metric)
    ggsave("tmp.pdf", width=9, height=9)
    embed_fonts("tmp.pdf", outfile=plot_pdf)
} else if (plot_type == "individual") {
    msg_type <- args[5]
    size <- strtoi(args[6])
    plot <- individual_plot(summarized, metric, size, msg_type)
    ggsave("tmp.pdf", width=9, height=6)
    embed_fonts("tmp.pdf", outfile=plot_pdf)
}


