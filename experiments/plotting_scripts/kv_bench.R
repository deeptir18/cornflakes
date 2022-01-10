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
d_postprocess <- read.csv(args[2], sep = ",", header = TRUE)
# argument 3: pdf to write plot into
plot_pdf <- args[3]
# argument 4: metric (p99, or median)
metric <- args[4]
# argument 5: plot type ([full, individual])
plot_type <- args[5]
# argument 6: if individual -- size
# argument 7: if individual -- num_values

# cut out all data where the percentachieved is less than .95
# d <- subset(d, percent_achieved_rate > .95)

labels <- c("protobuf" = "Protobuf", "capnproto" = "Capnproto", "flatbuffers" = "Flatbuffers", "cereal" = "Cereal", "cornflakes-dynamic" = "Cornflakes (Hardware SG)", "cornflakes1c-dynamic" = "Cornflakes (1 Software Copy)")

shape_values <- c('protobuf' = 7, 'cereal' = 4, 'capnproto' = 18, 'flatbuffers' = 17, 'cornflakes1c-dynamic' = 15, 'cornflakes-dynamic' = 19)
#size_values <- c('protobuf' = 4, 'cereal' = 4, 'capnproto' = 3.5, 'flatbuffers' = 2.5, 'cornflakes-dynamic' = 3.5, 'cornflakes1c-dynamic' = 2.5)

color_values <- c('cornflakes-dynamic' = '#1b9e77', 
                    'cornflakes1c-dynamic' = '#d95f02',
                    'flatbuffers' = '#7570b3',
                    'capnproto' = '#e7298a',
                    'cereal' = '#66a61e',
                    'protobuf' = '#e6ab02')

d$serialization <- factor(d$serialization, levels = c('cereal', 'flatbuffers', 'capnproto', 'protobuf', 'cornflakes1c-dynamic', 'cornflakes-dynamic'))

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
            coord_cartesian(ylim=c(0, y_cutoff), xlim=c(0, 32)) +
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
            scale_shape_manual(values = shape_values, labels = labels) +
            scale_color_manual(values = color_values ,labels = labels) +
            scale_fill_manual(values = color_values, labels = labels) +
            theme_light() +
            scale_x_continuous(n.breaks=8) +
            expand_limits(x = 0, y = 0) +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans"),
                  legend.title = element_blank(),
                  legend.key.size = unit(2, 'mm'),
                  legend.box="vertical",
                  legend.spacing.x = unit(0.15, 'cm'),
                  legend.spacing.y = unit(0.05, 'cm'),
                  legend.text=element_text(size=11),
                  axis.title=element_text(size=11,face="plain", colour="#000000"),
                  axis.text=element_text(size=11, colour="#000000"),
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

tput_plot <- function(data) {
    y_name <- "maxtputgbps"
    y_label <- "round(maxtputgbps, 2)"
    y_label_height <- "maxtputgbps + maxtputgbpssd"
    y_axis <- "Highest Achieved\nLoad (Gbps)"
    y_min = "maxtputgbps - maxtputgbpssd"
    y_max = "maxtputgbps + maxtputgbpssd"
    data$serialization <- factor(data$serialization, levels = c("capnproto", "flatbuffers", "cornflakes1c-dynamic", "cornflakes-dynamic"))
    plot <- ggplot(data,
                    aes_string(x = "factor(num_values)",
                        y=y_name,
                        fill = "serialization")) +
                   expand_limits(y = 0) +
            geom_point(size = 3, stroke=0.2, position=position_dodge(0.3), stat="identity", aes(color=serialization, shape=serialization,fill=serialization, size=serialization)) +
            geom_bar(position=position_dodge(0.3), stat="identity", width = 0.05) +
            guides(colour=guide_legend(nrow=2, byrow=TRUE),
                   #fill=guide_legend(nrow=2, byrow=TRUE),
                   shape=guide_legend(nrow=2, byrow=TRUE)) +
                   #size=guide_legend(nrow=2,byrow=TRUE)) +
            scale_color_manual(values = color_values ,labels = labels) +
            scale_fill_manual(values = color_values, labels = labels, guide = FALSE) +
            scale_shape_manual(values = shape_values, labels = labels) +
            # scale_size_manual(values = size_values, labels = labels) +
            scale_y_continuous(expand = expansion(mult = c(0, .2))) +
            labs(x = "Number of Batched Gets", y = y_axis) +
            theme_light() +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans"),
                  legend.title = element_blank(),
                  legend.key.size = unit(2, 'mm'),
                  legend.spacing.x = unit(0.1, 'cm'),
                  legend.spacing.y = unit(0.05, 'cm'),
                  legend.text=element_text(size=11),
                  axis.title=element_text(size=11,face="plain", colour="#000000"),
                  axis.text.y=element_text(size=11, colour="#000000"),
                  axis.text.x=element_text(size=11, colour="#000000", angle=0),
                  legend.margin=margin(0,0,0,0),
                  legend.box.margin=margin(-5,-10,-5,-10))
    print(plot)
    return(plot)
}


d$total_size = d$value_size * d$num_values;
summarized <- ddply(d, c("serialization", "total_size", "value_size", "num_values", "offered_load_pps", "offered_load_gbps"), summarise,
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
    print(d_postprocess)
    plot <- tput_plot(d_postprocess)
    print(plot_pdf)
    ggsave("tmp.pdf", width=5, height=2)
    embed_fonts("tmp.pdf", outfile=plot_pdf)
}

