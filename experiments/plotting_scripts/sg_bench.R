#!/usr/bin/env Rscript

library(ggplot2)
library(plyr)
library(tidyr)
library(extrafont)
library(showtext)
library(viridis)
font_add_google("Fira Sans")
showtext_auto()

x_labels_arrays <- c("65536" = "65536\nIn L1", "819200" = "819200\nIn L2", "4096000" = "4096K\nIn L3", "65536000" = "65536K\n~4X L3", "655360000" = "655360K\n~40x L3")
x_labels_recv_size <- c("256" = "256", "512" = "512", "1024" = "1024", "2048" = "2048", "4096" = "4096")

x_labels_total_size <- c("256" = "256", "512" = "512", "1024" = "1024", "2048" = "2048", "4096" = "4096")
x_labels_num_segments <- c("1" = "1", "2" = "2", "4" = "4", "8" = "8", "16" = "16", "32"="32")

x_labels_none <- c()

x_axis_labels_func <-function(factor) {
    print(factor)
    if (factor == "array_size") {
        return(x_labels_arrays)
    } else if (factor == "recv_size") {
        return(x_labels_recv_size)
    } else if (factor == "num_segments") {
        return(x_labels_num_segments)
    } else if (factor == "total_size") {
        return(x_labels_total_size)
    } else {
        # for other factors, names don't matter
        return(x_labels_none)
    }
}

x_axis_name_func <- function(factor) {
    if (factor == "array_size") {
        return("Working Set Size (Bytes)")
    } else if (factor == "recv_size") {
        return("Received Packet Size (Bytes)")
    } else if (factor == "num_segments") {
        return("Number of Segments")
    } else if (factor == "total_size") {
        return("Total Packet Size (Bytes)")
    }
    return("")
}


shape_values <- c('scatter_gather' = 19, 'copy_each_segment' = 15)

color_values <- c('scatter_gather' = '#1b9e77', 
                    'copy_each_segment' = '#d95f02')

args <- commandArgs(trailingOnly=TRUE)
d_postprocess <- read.csv(args[2], sep=",", header= TRUE, quote="",  encoding="UTF-8")
d <- read.csv(args[1], sep=",", header = TRUE,  quote="",  encoding="UTF-8")
plot_pdf <- args[3]
metric <- args[4]
plot_type <- args[5] # full OR individual OR tput-latency
factor <- args[6] # array_size or recv_size or num_segments or total_Size
# argument 6: if individual -- total size
# argument 7: if individual -- num segments
# argument 8: if tput-latency -- array size or recv_size or num_segments or
# total_size

# convert to us
d$p99 <- d$p99 / 1000.0
d$median <- d$median / 1000.0
d$offered_load_pps <- d$offered_load_pps / 1000000.0
d$achieved_load_pps <- d$achieved_load_pps / 1000000.0
d_postprocess$mp99 <- d_postprocess$mp99 / 1000.0
d_postprocess$p99sd <- d_postprocess$p99sd / 1000.0
d_postprocess$mmedian <- d_postprocess$mmedian / 1000.0
d_postprocess$mediansd <- d_postprocess$mediansd / 1000.0
d_postprocess$maxtputpps <- d_postprocess$maxtputpps / 1000000.0
d_postprocess$maxtputppssd <- d_postprocess$maxtputppssd / 1000000.0

labels <- c("scatter_gather" = "Scatter-Gather", "copy_each_segment" = "Copy Segments")

# add in the row name
get_system_name <- function(row) {
    if (row["with_copy"] == "False" && row["as_one"] == "False") {
        res <- "scatter_gather"
    }
    else if (row["with_copy"] == "True" && row["as_one"] == "False") {
        res <- "copy_each_segment"
    }
    return(res)
}

base_median_plot <- function(data, factor_name, x_axis_name, x_axis_labels) {
    factor_name_string <- sprintf("factor(%s)", factor_name)
    plot <- ggplot(data,
                  aes_string(x = factor_name_string,
                      y = "mmedian",
                      fill  = "system_name")) +
            expand_limits(y = 0) +
            geom_bar(position="dodge", stat="identity", width = 0.5) +
            geom_errorbar(aes(ymin=mmedian-mediansd, ymax=mmedian+mediansd),position="dodge", stat="identity", width = 0.5) +
            scale_fill_viridis_d(labels = labels) +
            scale_color_viridis_d(labels = labels) +
            geom_text(data, 
                      mapping = aes_string(x=factor_name_string,
                                    y = "mmedian + mediansd",
                                    label = "round(mmedian, 2)",
                                    family = "'Fira Sans'",
                                    vjust = "0.0",
                                    hjust = "0.0",
                                    angle = "45"), 
                      size = 6,
                      position = position_dodge2(width = 0.5, preserve = "single")) +
            scale_y_continuous(expand = expansion(mult = c(0, .15))) +
            scale_x_discrete(labels = x_axis_labels) +
            labs(x = x_axis_name, y = "Median Latency (µs)")

    print(plot)
    return(plot)
}
base_p99_plot <- function(data, factor_name, x_axis_name, x_axis_labels) {
    factor_name_string <- sprintf("factor(%s)", factor_name)
    plot <- ggplot(data,
                  aes_string(x = factor_name_string,
                      y = "mp99",
                      fill  = "system_name")) +
            expand_limits(y = 0) +
            geom_bar(position="dodge", stat="identity", width = 0.5) +
            geom_errorbar(aes(ymin=mp99-p99sd, ymax=mp99+p99sd),position="dodge", stat="identity", width = 0.5) +
            scale_fill_viridis_d(labels = labels) +
            scale_color_viridis_d(labels = labels) +
            geom_text(data, 
                      mapping = aes_string(x=factor_name_string,
                                    y = "mp99 + p99sd",
                                    label = "round(mp99, 2)",
                                    family = "'Fira Sans'",
                                    vjust = "0.0",
                                    hjust = "0.0",
                                    angle = "45"), 
                      size = 6,
                      position = position_dodge2(width = 0.5, preserve = "single")) +
            scale_y_continuous(expand = expansion(mult = c(0, .15))) +
            scale_x_discrete(labels = x_axis_labels) +
            labs(x = x_axis_name, y = "p99 Latency (µs)")

    print(plot)
    return(plot)
}

base_tput_plot <- function(data, factor_name, x_axis_name, x_axis_labels, metric) {
    factor_name_string <- sprintf("factor(%s)", factor_name)
    y_name <- "maxtputpps"
    y_label <- "round(maxtputpps, 2)"
    y_label_height <- "maxtputpps + maxtputppssd"
    y_axis <- "Highest Achieved Load\n(100K Requests / sec)"
    y_min = "maxtputpps - maxtputppssd"
    y_max = "maxtputpps + maxtputppssd"
    if (metric == "tput_gbps") {
        y_name <- "maxtputgbps"
        y_label <- "round(maxtputgbps, 2)"
        y_label_height <- "maxtputgbps + maxtputgbpssd"
        y_axis <- "Highest Achieved Load\n(Gbps)"
        y_min = "maxtputgbps - maxtputgbpssd"
        y_max = "maxtputgbps + maxtputgbpssd"
    }
    plot <- ggplot(data,
                   aes_string(x=factor_name_string,
                       y = y_name,
                       fill = "system_name",
                       color = "system_name",
                       group = "system_name",
                       shape = "system_name")) +
                expand_limits(y = 0) +
            geom_point(size = 4) + geom_line(size = 1) +
            scale_shape_manual(values = shape_values, labels = labels) +
            scale_color_manual(values = color_values, labels = labels) +
            scale_fill_manual(values = color_values, labels = labels) +
            scale_x_discrete(labels = x_axis_labels) +
            # add space for the labels
            # scale_y_continuous(expand = expansion(mult = c(0, .15))) +
            labs(x = x_axis_name, y = y_axis)
}

label_plot <- function(plot) {
    plot <- plot +
            theme_light() +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans"),
                  legend.title = element_blank(),
                  legend.key.size = unit(2, 'mm'),
                  legend.spacing.x = unit(0.1, 'cm'),
                  legend.text=element_text(size=18),
                  axis.title=element_text(size=18,face="plain", colour="#000000"),
                  axis.text.y=element_text(size=18, colour="#000000"),
                  axis.text.x=element_text(size=16, colour="#000000", angle=0))
}

base_p99_tput_latency <- function(data, y_cutoff) {
    # print(data)
    plot <- ggplot(data,
                    aes(x = machieved_load_pps,
                        y = mp99,
                        color = system_name,
                        shape = system_name,
                        ymin = mp99 - p99sd,
                        ymax = mp99 + p99sd)) +
                coord_cartesian(ylim = c(0, y_cutoff), expand = FALSE) +
    labs(x = "Achieved Load (100K Requests/sec)", y = "p99 Latency (µs)")
    return(plot)
}

base_median_tput_latency <- function(data, y_cutoff) {
    # print(data)
    plot <- ggplot(data,
                    aes(x = machieved_load_pps,
                        y = mmedian,
                        color = system_name,
                        shape = system_name,
                        ymin = mmedian - mediansd,
                        ymax = mmedian + mediansd)) +
                coord_cartesian(ylim = c(0, y_cutoff), expand = FALSE) +
    labs(x = "Achieved Load (100K Requests/sec)", y = "Median Latency (µs)")
    return(plot)

}

label_tput_latency <- function(plot) {
    plot <- plot +
            geom_point(size=4) +
            geom_line(size = 1, aes(color=system_name)) +
            scale_shape_manual(values = shape_values, labels = labels) +
            scale_color_manual(values = color_values ,labels = labels) +
            scale_fill_manual(values = color_values, labels = labels) +
            theme_light() +
            expand_limits(x = 0, y = 0) +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans", size = 27),
                  legend.title = element_blank(),
                  legend.key.size = unit(15, 'mm'),
                  legend.spacing.x = unit(0.1, 'cm'),
                  axis.title=element_text(size=27,face="plain", colour="#000000"),
                  axis.text=element_text(size=27, colour="#000000"))
    return(plot)
}

base_tput_latency_plot <- function(data, metric) {
    if (metric == "p99") {
        base_plot <- base_p99_tput_latency(data, 100)
        base_plot <- label_tput_latency(base_plot)
        return(plot)
    } else if (metric == "median") {
        base_plot <- base_median_tput_latency(data, 50)
        base_plot <- label_tput_latency(base_plot)
        return(plot)
    }
}

base_plot <- function(data, metric, factor_name) {
    if (metric == "p99") {
        base_plot <- base_p99_plot(data, factor_name, x_axis_name_func(factor_name), x_axis_labels_func(factor_name))
        base_plot <- label_plot(base_plot)
        return(base_plot)
    } else if (metric == "median") {
        base_plot <- base_median_plot(data, factor_name, x_axis_name_func(factor_name), x_axis_labels_func(factor_name))
        base_plot <- label_plot(base_plot)
        return(base_plot)
    } else if (metric == "tput_gbps" | metric == "tput_pps") {
        base_plot <- base_tput_plot(data, factor_name, x_axis_name_func(factor_name), x_axis_labels_func(factor_name), metric)
        base_plot <- label_plot(base_plot)
        return(base_plot)
    }
}

individual_plot_segments <- function(data, metric, size) {
    data <- subset(data, total_size == size)
    plot <- base_plot(data, metric, "num_segments")
    print(plot)
    return(plot)
}

individual_plot_size <- function(data, metric, segments) {
    data <- subset(data, num_segments == segments)
    plot <- base_plot(data, metric, "total_size")
    print(plot)
    return(plot)
}

individual_plot_factor <- function(data, metric, size, values, factor_name) {
    data <- subset(data, num_segments == values & total_size == size)
    # print(data)
    plot <- base_plot(data, metric, factor_name)
    print(plot)
    return(plot)
}

# subset data BEFORE passing into this function
tput_latency_plot <- function(data, metric) {
    plot <- base_tput_latency_plot(data, metric)
    print(plot)
    return(plot)
}

full_plot <- function(data, metric, factor_name) {
    plot <- base_plot(data, metric, factor_name) +
        facet_grid(total_size ~ num_segments) +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans"),
                  legend.title = element_blank(),
                  legend.key.size = unit(10, 'mm'),
                  legend.spacing.x = unit(0.1, 'cm'),
                  axis.title=element_text(size=27,face="plain", colour="#000000"),
                  axis.text=element_text(size=4, colour="#000000"))

    print(plot)
    return(plot)
}

if (!("recv_size" %in% colnames(d)))
{
    d$recv_size <- 0
}

d$system_name <- apply(d, 1, get_system_name)
d$total_size <- d$segment_size * d$num_segments
summarized <- ddply(d, c("system_name", "segment_size", "num_segments", "with_copy", "as_one", "total_size", "array_size", "recv_size", "offered_load_pps", "offered_load_gbps"),
                    summarise,
                    mavg = mean(avg),
                    mmedian = mean(median),
                    mediansd = sd(median),
                    mp99 = mean(p99),
                    p99sd = sd(p99),
                    mp999 = mean(p999),
                    machieved_load_pps = mean(achieved_load_pps),
                    machieved_load_gbps = mean(achieved_load_gbps))
d_postprocess$system_name <- apply(d_postprocess, 1, get_system_name)
d_postprocess$total_size <- d_postprocess$segment_size * d_postprocess$num_segments


if (plot_type == "full") {
    # factor should only be recv_size or array_size
    plot <- full_plot(d_postprocess, metric, factor)
    ggsave("tmp.pdf", width=8, height=11)
    embed_fonts("tmp.pdf", outfile=plot_pdf)
} else if (plot_type == "individual") {
    total_size <- strtoi(args[7])
    num_segments <- strtoi(args[8])
    if (factor == "num_segments") {
        plot <- individual_plot_segments(d_postprocess, metric, total_size)
        ggsave("tmp.pdf", width=5, height=3)
        embed_fonts("tmp.pdf", outfile=plot_pdf)
    } else if (factor == "total_size") {
        print(num_segments)
        plot <- individual_plot_size(d_postprocess, metric, num_segments)
        ggsave("tmp.pdf", width=5, height=3)
        embed_fonts("tmp.pdf", outfile=plot_pdf)
    } else if (factor == "recv_size") {
        plot <- individual_plot_factor(d_postprocess, metric, total_size, num_segments, factor)
        ggsave("tmp.pdf", width=5, height=3)
        embed_fonts("tmp.pdf", outfile=plot_pdf)

    } else {
        # array size
        plot <- individual_plot_factor(d_postprocess, metric, total_size, num_segments, factor)
        ggsave("tmp.pdf", width=5, height=4)
        embed_fonts("tmp.pdf", outfile=plot_pdf)
    }
} else if (plot_type == "tput_latency") {
    # subset by all the args
    size <- strtoi(args[7])
    segments <- strtoi(args[8])
    summarized <- subset(summarized, num_segments == segments & total_size == size)
    if (factor == "array_size") {
        arrays <- strtoi(args[9])
        summarized <- subset(summarized, array_size == arrays)
    } else if (factor == "recv_size") {
        recv <- strtoi(args[9])
        summarized <- subset(summarized, recv_size == recv)
    }
    plot <- tput_latency_plot(summarized, metric)
    ggsave("tmp.pdf", width = 9, height=6)
    embed_fonts("tmp.pdf", outfile=plot_pdf)
}
