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
d_postprocess <- read.csv(args[2], sep=",", header= TRUE, quote="",  encoding="UTF-8")
# argument 2: pdf to write plot into
plot_pdf <- args[3]
# argument 3: metric (p99, or median)
metric <- args[4]
# argument 4: plot type ([full, individual])
plot_type <- args[5]
# argument 5: if individual -- message type
# argument 6: if individual -- size (of message total)
p99_x_cutoff <- 100

labels <- c("protobuf" = "Protobuf", "capnproto" = "Capnproto", "flatbuffers" = "Flatbuffers", "cereal" = "Cereal", "cornflakes-dynamic" = "Cornflakes (Hardware SG)", "cornflakes1c-dynamic" = "Cornflakes (1 Software Copy)", "ideal" = "Peak Single-Core", "onecopy" = "One Copy", "twocopy" = "Two Copies")


shape_values <- c('protobuf' = 8, 'cereal' = 4, 'capnproto' = 18, 'flatbuffers' = 17, 'cornflakes1c-dynamic' = 15, 'cornflakes-dynamic' = 19, "ideal" = 20, "onecopy" = 1, "twocopy" = 10)

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

base_plot <- function(data, metric, x_cutoff) {
    if (metric == "p99") {
        base_plot <- base_p99_plot(data, 100.0, x_cutoff)
        base_plot <- label_plot(base_plot)
    } else if (metric == "median") {
        base_plot <- base_median_plot(data, 50.0, x_cutoff)
        base_plot <- label_plot(base_plot)
    }
}

base_p99_plot <- function(data, y_cutoff, x_cutoff) {
    plot <- ggplot(data,
                    aes(x = maloadgbps,
                        y = mp99,
                        color = serialization,
                        shape = serialization,
                        ymin = mp99 - sdp99,
                        ymax = mp99 + sdp99)) +
            coord_cartesian(ylim=c(0, y_cutoff), xlim=c(0, x_cutoff)) +
    labs(x = "Achieved Load (Gbps)", y = "p99 Latency (µs)")
    return(plot)
}

base_median_plot <- function(data, y_cutoff, x_cutoff) {
    plot <- ggplot(data,
                    aes(x = maloadgbps,
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
            geom_point(size=2) +
            geom_line(size = 0.5, aes(color=serialization)) +
            scale_shape_manual(values = shape_values, labels = labels) +
            scale_color_manual(values = color_values ,labels = labels) +
            scale_fill_manual(values = color_values, labels = labels) +
            theme_light() +
            expand_limits(x = 0, y = 0) +
            scale_x_continuous(n.breaks=5) +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans"),
                  legend.title = element_blank(),
                  legend.key.size = unit(2, 'mm'),
                  legend.spacing.x = unit(0.3, 'cm'),
                   legend.text=element_text(size=11),
                  legend.margin=margin(0,0,0,0),
                    legend.box.margin=margin(-5,-10,-5,-10),
                axis.title=element_text(size=11,face="plain", colour="#000000"),
                  axis.text=element_text(size=11, colour="#000000"))
    return(plot)
}

individual_plot <- function(data, metric, total_size, msg_type) {
    data <- subset(data, message_type == msg_type & size == total_size)
    print(total_size)
    if (total_size == 4096) {
        print("reaching x cutoff 4096")
        p99_x_cutoff <- 30
    } else if (total_size == 512) {
        print("reaching x cutoff")
        p99_x_cutoff <- 4
    }
    plot <- base_plot(data, metric, p99_x_cutoff)
    print(plot)
    return(plot)
}

full_plot <- function(data, metric) {
    plot <- base_plot(data, metric, p99_x_cutoff) +
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
tput_plot <- function(data) {
    y_name <- "maxtputgbps"
    y_label <- "round(maxtputgbps, 2)"
    y_label_height <- "maxtputgbps + maxtputgbpssd"
    y_axis <- "Highest Achieved\nLoad (Gbps)"
    y_min = "maxtputgbps - maxtputgbpssd"
    y_max = "maxtputgbps + maxtputgbpssd"
    data$serialization <- factor(data$serialization, levels = c("cereal", "capnproto", "protobuf", "flatbuffers", "cornflakes1c-dynamic", "cornflakes-dynamic"))
    plot <- ggplot(data,
                    aes_string(x = "message_type",
                        y=y_name,
                        fill = "serialization")) +
                   expand_limits(y = 0) +
            # geom_segment( aes_string(x="message_type", xend="message_type", y=
             #                        "0", yend=y_name), position=position_dodge(0.5), stat="identity") +
            geom_point( size=1, stroke=1, position=position_dodge(0.5), stat="identity", aes(color=serialization, shape=serialization,fill=serialization)) +

            geom_bar(position=position_dodge(0.5), stat="identity", width = 0.1) +
            scale_color_manual(values = color_values ,labels = labels) +
            scale_fill_manual(values = color_values, labels = labels, guide = FALSE) +
            scale_shape_manual(values = shape_values, labels = labels) +
            scale_y_continuous(expand = expansion(mult = c(0, .2))) +
            labs(x = "Message Type", y = y_axis) +
            theme_light() +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans"),
                  legend.title = element_blank(),
                  legend.key.size = unit(2, 'mm'),
                  legend.spacing.x = unit(0.1, 'cm'),
                  legend.text=element_text(size=11),
                  legend.margin=margin(0,0,0,0),
                legend.box.margin=margin(-5,-10,-5,-10),
                  axis.title=element_text(size=11,face="plain", colour="#000000"),
                  axis.text.y=element_text(size=11, colour="#000000"),
                  axis.text.x=element_text(size=11, colour="#000000", angle=0))
    print(plot)
    return(plot)
}
if (!("ideal" %in% colnames(d))) {
    #d$serialization <- factor(d$system, levels = c('cereal', 'flatbuffers', 'capnproto', 'protobuf', 'cornflakes1c-dynamic', 'cornflakes-dynamic', 'twocopy','onecopy', 'ideal'))
} else {
    #d$serialization <- factor(d$system, levels = c('cereal', 'flatbuffers', 'capnproto', 'protobuf', 'cornflakes1c-dynamic', 'cornflakes-dynamic'))
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
} else if (plot_type == "motivation") {
    summarized$serialization <- factor(summarized$serialization, levels = c('cereal', 'flatbuffers', 'capnproto', 'protobuf', 'twocopy', 'onecopy', 'ideal'))
    msg_type <- args[6]
    size <- strtoi(args[7])
    plot <- individual_plot(summarized, metric, size, msg_type)
    ggsave("tmp.pdf", width=5, height=3)
    embed_fonts("tmp.pdf", outfile=plot_pdf)

} else if (plot_type == "individual") {
    summarized$serialization <- factor(summarized$serialization, levels = c('cereal', 'flatbuffers', 'capnproto', 'protobuf', 'cornflakes1c-dynamic', 'cornflakes-dynamic'))
    msg_type <- args[6]
    size <- strtoi(args[7])
    plot <- individual_plot(summarized, metric, size, msg_type)
    ggsave("tmp.pdf", width=5, height=2)
    embed_fonts("tmp.pdf", outfile=plot_pdf)
} else if (plot_type == "list-compare") {
    size_arg <- strtoi(args[6])
    d_postprocess <- subset(d_postprocess, (message_type == "single") | (message_type == "list-2" ) | (message_type == "list-4") | (message_type == "list-8"))
    d_postprocess <- subset(d_postprocess, size == size_arg)
    d_postprocess$message_type <- factor(d_postprocess$message_type, levels = c("single", "list-2", "list-4", "list-8"))
    plot <- tput_plot(d_postprocess)
    ggsave("tmp.pdf", width=5, height=2)
    embed_fonts("tmp.pdf", outfile=plot_pdf)
} else if (plot_type == "tree-compare") {
    size_arg <- strtoi(args[6])
    d_postprocess <- subset(d_postprocess, (message_type == "list-2") | (message_type == "tree-1") | (message_type == "list-4") | (message_type == "tree-2"))
    d_postprocess$message_type <- factor(d_postprocess$message_type, levels = c("list-2", "tree-1", "list-4", "tree-2"))
    d_postprocess <- subset(d_postprocess, size == size_arg)
    plot <- tput_plot(d_postprocess)
    ggsave("tmp.pdf", width=5, height=2)
    embed_fonts("tmp.pdf", outfile=plot_pdf)
}


