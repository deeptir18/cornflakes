#!/usr/bin/env Rscript

library(ggplot2)
library(plyr)
library(tidyr)
library(extrafont)
library("stringr")  
library(showtext)
library(viridis)
font_add_google("Fira Sans")
showtext_auto()

args <- commandArgs(trailingOnly=TRUE)
d <- read.csv(args[1], sep=",", header = TRUE)
d_postprocess <- read.csv(args[2], sep=",", header= TRUE, quote="",  encoding="UTF-8")
# argument 2: pdf to write plot into
plot_pdf <- args[3]
basename <- sub('\\.pdf$', '', plot_pdf) 
cr_plot_pdf <- paste(basename, "cr.pdf", sep = "_")
anon_plot_pdf <- paste(basename, "anon.pdf", sep = "_")
# argument 3: metric (p99, or median)
metric <- args[4]
# argument 4: plot type ([full, individual])
plot_type <- args[5]
# argument 5: if individual -- message type
# argument 6: if individual -- size (of message total)

# subset d to be points where `percent_achieved_rate > .95`
d <- d[ which(d$percent_achieved_rate > 0.95),]

cr_labels <- c("protobuf" = "Protobuf", "capnproto" = "Capnproto", "flatbuffers" = "Flatbuffers", "cornflakes-dynamic" = "Cornflakes (SG)", "cornflakes1c-dynamic" = "Cornflakes (Copy)", "ideal" = "Peak Single Core", "onecopy" = "One Copy", "twocopy" = "Two Copies", "manualzerocopy" = "Manual Zero Copy")
anon_labels <- c("protobuf" = "Protobuf", "capnproto" = "Capnproto", "flatbuffers" = "Flatbuffers", "cornflakes-dynamic" = "AnonSys (SG)", "cornflakes1c-dynamic" = "AnonSys (Copy)", "ideal" = "Peak Single Core", "onecopy" = "One Copy", "twocopy" = "Two Copies", "manualzerocopy" = "Manual Zero Copy")


shape_values <- c('protobuf' = 8, 'capnproto' = 18, 'flatbuffers' = 17, 'cornflakes1c-dynamic' = 15, 'cornflakes-dynamic' = 19, "ideal" = 20, "onecopy" = 1, "twocopy" = 10, "manualzerocopy" = 13)

color_values <- c('cornflakes-dynamic' = '#1b9e77', 
                    'cornflakes1c-dynamic' = '#d95f02',
                    "ideal" = '#252525',
                    "manualzerocopy" = "#636363",
                    "onecopy" = '#969696',
                    "twocopy" = "#cccccc",
                    'flatbuffers' = '#7570b3',
                    'capnproto' = '#e7298a',
                    'protobuf' = '#e6ab02')
options(width=10000)
levels <- c('capnproto', 'protobuf', 'flatbuffers', 'twocopy', 'onecopy', 'manualzerocopy', 'ideal', 'cornflakes1c-dynamic', 'cornflakes-dynamic')

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
cr_labels <- subset_named(cr_labels, unique_serialization_labels)
anon_labels <- subset_named(anon_labels, unique_serialization_labels)
levels <- subset_flat(levels, unique_serialization_labels)

d$serialization <- factor(d$serialization, levels = levels)
d_postprocess$serialization <- factor(d_postprocess$serialization, levels = levels)

base_plot <- function(data, metric,labels ) {
    if (metric == "p99") {
        base_plot <- base_p99_plot(data, 100.0)
        base_plot <- label_plot(base_plot,labels)
    } else if (metric == "median") {
        base_plot <- base_median_plot(data, 50.0)
        base_plot <- label_plot(base_plot,labels)
    }
}

base_p99_plot <- function(data, y_cutoff) {
    plot <- ggplot(data,
                    aes(x = maloadgbps,
                        y = mp99,
                        color = serialization,
                        shape = serialization)) +
            coord_cartesian(ylim=c(0, y_cutoff)) +
    labs(x = "Achieved Load (Gbps)", y = "p99 Latency (µs)")
    return(plot)
}

base_median_plot <- function(data, y_cutoff, x_cutoff) {
    plot <- ggplot(data,
                    aes(x = maloadgbps,
                        y = mmedian,
                        color = serialization,
                        shape = serialization)) +
            coord_cartesian(ylim=c(0, y_cutoff), expand = FALSE) +
    labs(x = "Offered Load (Gbps)", y = "Median Latency (µs)")
    return(plot)
}

label_plot <- function(plot,labels) {
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
                  #legend.spacing.x = unit(0.3, 'cm'),
                   legend.text=element_text(size=11),
                  legend.margin=margin(0,0,0,0),
                    legend.box.margin=margin(-5,-10,-5,-10),
                axis.title=element_text(size=11,face="plain", colour="#000000"),
                  axis.text=element_text(size=11, colour="#000000")) +
            guides(colour=guide_legend(nrow=2, byrow=TRUE),
                   fill=guide_legend(nrow=2, byrow=TRUE),
                   shape=guide_legend(nrow=2, byrow=TRUE))
            
    return(plot)
}

individual_plot <- function(data, metric, total_size, msg_type,labels) {
    data <- subset(data, message_type == msg_type & size == total_size)
    plot <- base_plot(data, metric,labels)
    print(plot)
    return(plot)
}

full_plot <- function(data, metric,labels) {
    plot <- base_plot(data, metric,labels) +
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
tput_plot <- function(data,labels, x_axis_name) {
    print(data$maxtputgbps)
    plot <- ggplot(data,
                    aes(x = reorder(factor_name, num_leaves),
                        y=maxtputgbps,
                        fill = serialization)) +
            geom_text(position = position_dodge(0.8),
                      stat = "identity",
                      aes(y = maxtputgbps + 9.5,
                      label = round(maxtputgbps, 1)),
                   size = 2.75,
                   angle = 70) +
                   expand_limits(y = 0) +
            geom_point( size=2, stroke=0.2, position=position_dodge(0.8), stat="identity", aes(color=serialization, shape=serialization,fill=serialization)) +
            geom_bar(position=position_dodge(0.8), stat="identity", width = 0.05) +
            guides(colour=guide_legend(nrow=2, byrow=TRUE),
                   shape=guide_legend(nrow=2, byrow=TRUE)) +
            scale_color_manual(values = color_values ,labels = labels, breaks = levels) +
            scale_fill_manual(values = color_values, labels = labels, guide = "none", breaks = levels) +
            scale_shape_manual(values = shape_values, labels = labels, breaks = levels) +
            scale_y_continuous(expand = expansion(mult = c(0, .2))) +
            labs(x = x_axis_name, y = "Highest Achieved\nLoad (Gbps)") +
            theme_light() +
            scale_x_discrete(labels = function(x) str_wrap(x, width = 12)) +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans"),
                  legend.title = element_blank(),
                  legend.key.size = unit(2, 'mm'),
                  legend.spacing.x = unit(0.1, 'cm'),
                  legend.spacing.y = unit(0.05, 'cm'),
                  legend.text=element_text(size=11),
                legend.box.margin=margin(-5,-10,-5,-10),
                  legend.margin=margin(0,0,0,0),
                  axis.title=element_text(size=11,face="plain", colour="#000000"),
                  axis.text.y=element_text(size=11, colour="#000000"),
                  axis.text.x=element_text(size=8, colour="#000000", angle=0))
    print(plot)
    return(plot)
}
summarized <- ddply(d, c("serialization", "size", "message_type", "offered_load_pps", "offered_load_gbps"),
                    summarise,
                    mavg = median(avg),
                    mp99 = median(p99),
                    mp999 = median(p999),
                    mmedian = median(median),
                    maloadgbps = median(achieved_load_gbps),
                    maload = median(achieved_load_pps))

if (plot_type == "full") {
    plot <- full_plot(summarized, metric,cr_labels)
    ggsave("tmp.pdf", plot = plot, width=9, height=9)
    embed_fonts("tmp.pdf", outfile=cr_plot_pdf)

    plot <- full_plot(summarized, metric,anon_labels)
    ggsave("tmp.pdf", plot = plot, width=9, height=9)
    embed_fonts("tmp.pdf", outfile=anon_plot_pdf)
} else if (plot_type == "individual") {
    summarized$serialization <- factor(summarized$serialization, levels = levels)
    msg_type <- args[6]
    size <- strtoi(args[7])
    plot <- individual_plot(summarized, metric, size, msg_type, cr_labels)
    ggsave("tmp.pdf", plot = plot, width=5.5, height=2)
    embed_fonts("tmp.pdf", outfile=cr_plot_pdf)
    
    plot <- individual_plot(summarized, metric, size, msg_type, anon_labels)
    ggsave("tmp.pdf", plot = plot, width=5.5, height=2)
    embed_fonts("tmp.pdf", outfile=anon_plot_pdf)
} else if (plot_type == "list-compare") {
    size_arg <- strtoi(args[6])
    x_axis_name <- args[7]
    print(size_arg)
    print(x_axis_name)
    plot <- tput_plot(d_postprocess, cr_labels, x_axis_name)
    ggsave("tmp.pdf", plot = plot, width=5, height=2)
    embed_fonts("tmp.pdf", outfile=cr_plot_pdf)

    plot <- tput_plot(d_postprocess, anon_labels, x_axis_name)
    ggsave("tmp.pdf", plot = plot, width=5, height=2)
    embed_fonts("tmp.pdf", outfile=anon_plot_pdf)
} else if (plot_type == "tree-compare") {
    size_arg <- strtoi(args[6])
    d_postprocess <- subset(d_postprocess, (message_type == "list-2") | (message_type == "tree-1") | (message_type == "list-4") | (message_type == "tree-2"))
    d_postprocess$message_type <- factor(d_postprocess$message_type, levels = c("list-2", "tree-1", "list-4", "tree-2"))
    d_postprocess <- subset(d_postprocess, size == size_arg)
    plot <- tput_plot(d_postprocess, cr_labels)
    ggsave("tmp.pdf", plot = plot, width=5, height=2)
    embed_fonts("tmp.pdf", outfile=cr_plot_pdf)
    
    plot <- tput_plot(d_postprocess, anon_labels)
    ggsave("tmp.pdf", plot = plot, width=5, height=2)
    embed_fonts("tmp.pdf", outfile=anon_plot_pdf)
}


