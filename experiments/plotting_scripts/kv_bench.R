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
# argument 1: data
d <- read.csv(args[1], sep=",", header = TRUE)
# argument 2: pdf to write plot into
plot_pdf <- args[2]
# argument 3: metric (p99, or median)
metric <- args[3]
# argument 4: plot type ([full, individual])
plot_type <- args[4]
# argument 5: if individual -- size
# argument 6: if individual -- num_values

# cut out all data where the percentachieved is less than .95
# d <- subset(d, percent_achieved_rate > .95)

labels <- c("protobuf" = "Protobuf", "capnproto" = "Capnproto", "flatbuffers" = "Flatbuffers", "cereal" = "Cereal", "cornflakes-dynamic" = "Cornflakes (Hardware SG)", "cornflakes1c-dynamic" = "Cornflakes (1 Software Copy)")

shape_values <- c('protobuf' = 7, 'cereal' = 4, 'capnproto' = 18, 'flatbuffers' = 17, 'cornflakes1c-dynamic' = 15, 'cornflakes-dynamic' = 19)

color_values <- c('cornflakes-dynamic' = '#1b9e77', 
                    'cornflakes1c-dynamic' = '#d95f02',
                    'flatbuffers' = '#7570b3',
                    'capnproto' = '#e7298a',
                    'cereal' = '#66a61e',
                    'protobuf' = '#e6ab02')

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
    print(data)
    plot <- ggplot(data,
                    aes(x = offered_load_pps,
                        y = mp99,
                        color = serialization,
                        shape = serialization,
                        ymin = mp99 - sdp99,
                        ymax = mp99 + sdp99)) +
            coord_cartesian(ylim=c(0, y_cutoff), expand = FALSE) +
    labs(x = "Throughput (Requests/sec)", y = "p99 Latency (µs)")
    return(plot)
}

base_median_plot <- function(data, y_cutoff) {
    plot <- ggplot(data,
                    aes(x = offered_load_pps,
                        y = avgmedian,
                        color = serialization,
                        shape = serialization,
                        ymin = avgmedian - sdmedian,
                        ymax = avgmedian + sdmedian)) +
            coord_cartesian(ylim=c(0, y_cutoff), expand = FALSE) +
    labs(x = "Throughput (Requests/sec)", y = "Median Latency (µs)")
    return(plot)
}

label_plot <- function(plot) {
    plot <- plot +
            geom_point(size=4) +
            geom_line(size = 1, aes(color=serialization)) +
            scale_shape_manual(values = shape_values, labels = labels) +
            scale_color_manual(values = color_values ,labels = labels) +
            scale_fill_manual(values = color_values, labels = labels) +
            theme_light() +
            expand_limits(x = 0, y = 0) +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans"),
                  legend.title = element_blank(),
                  legend.key.size = unit(10, 'mm'),
                  legend.spacing.x = unit(0.1, 'cm'),
                  axis.title=element_text(size=27,face="plain", colour="#000000"),
                  axis.text=element_text(size=27, colour="#000000"))
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


d$total_size = d$value_size * d$num_values;
summarized <- ddply(d, c("serialization", "total_size", "value_size", "num_values", "offered_load_pps"), summarise,
                        mavg = mean(avg),
                        mp99 = mean(p99),
                        avgmedian = mean(median),
                        mp999 = mean(p999),
                        sdp99 = sd(p99),
                        sdmedian = sd(median),
                        mprate = mean(percent_achieved_rate),
                        maload = mean(achieved_load_pps))

# cutoff points where the sd of the p99 is > 50 % of the p99?
# summarized$sdp99_percent = summarized$sdp99 / summarized$mp99
# summarized <- subset(summarized, summarized$sdp99_percent < .25)

if (plot_type == "full") {
    plot <- full_plot(summarized, metric)
    ggsave("tmp.pdf", width=9, height=9)
    embed_fonts("tmp.pdf", outfile=plot_pdf)
} else if (plot_type == "individual") {
    total_size <- strtoi(args[5])
    num_values <- strtoi(args[6])
    plot <- individual_plot(summarized, metric, total_size, num_values)
    ggsave("tmp.pdf", width=9, height=6)
    embed_fonts("tmp.pdf", outfile=plot_pdf)
}

