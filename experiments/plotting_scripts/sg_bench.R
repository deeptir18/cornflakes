#!/usr/bin/env Rscript

library(ggplot2)
library(plyr)
library(tidyr)
library(extrafont)
library(showtext)
library(viridis)
font_add_google("Fira Sans")
showtext_auto()
labels <- c("scatter_gather" = "Scatter Gather", "copy_each_segment" = "Copy")

shape_values <- c('scatter_gather' = 15, 'copy_each_segment' = 19)

color_values <- c('scatter_gather' = '#1b9e77', 
                    'copy_each_segment' = '#d95f02')

args <- commandArgs(trailingOnly=TRUE)
d_postprocess <- read.csv(args[2], sep=",", header= TRUE, quote="",  encoding="UTF-8")
print("Read d postproces")
d <- read.csv(args[1], sep=",", header = TRUE,  quote="",  encoding="UTF-8")
print("Read d")
plot_pdf <- args[3]
metric <- args[4]
plot_type <- args[5]
# argument 6: if individual -- total size
# argument 7: if individual -- num segments
# argument 8: if tput-latency -- array size or cpu cycles

# convert to us
d$p99 <- d$p99 / 1000.0
d$median <- d$median / 1000.0
d$offered_load_pps <- d$offered_load_pps / 1000000.0
d$achieved_load_pps <- d$achieved_load_pps / 1000000.0
d_postprocess$mp99 <- d_postprocess$mp99 / 1000.0
d_postprocess$p99sd <- d_postprocess$p99sd / 1000.0
d_postprocess$mmedian <- d_postprocess$mmedian / 1000.0
d_postprocess$mediansd <- d_postprocess$mediansd / 1000.0
d$offered_load_pps <- d$offered_load_pps / 1000000.0
d$achieved_load_pps <- d$achieved_load_pps / 1000000.0

labels <- c("scatter_gather" = "Scatter Gather", "copy_each_segment" = "Copy Segments")

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
WIDTH <- 0.9

base_median_plot <- function(data) {
    plot <- ggplot(data,
                  aes(x = factor(array_size),
                      y = mmedian,
                      fill  = system_name)) +
            expand_limits(y = 0) +
            geom_bar(position="dodge", stat="identity", width = 0.9) +
            geom_errorbar(aes(ymin=mmedian-mediansd, ymax=mmedian+mediansd),position="dodge", stat="identity") +
            scale_fill_viridis_d() +
            scale_color_viridis_d() +
            geom_text(data, 
                      mapping = aes(x=factor(array_size), 
                                    y = mmedian + mediansd,
                                    label = mmedian,
                                    family = "Fira Sans",
                                    vjust = -0.45,
                                    hjust = -0.1,
                                    angle = 45), 
                      size = 2,
                      position = position_dodge2(width = 0.9, preserve = "single")) +
            labs(x = "Working Set Size (bytes)", y = "Median Latency (µs)")

    print(plot)
    return(plot)
}
base_p99_plot <- function(data) {
    plot <- ggplot(data,
                  aes(x = factor(array_size),
                      y = mp99,
                      fill  = system_name)) +
            expand_limits(y = 0) +
            geom_bar(position="dodge", stat="identity", width = 0.9) +
            geom_errorbar(aes(ymin=mp99-p99sd, ymax=mp99+p99sd),position="dodge", stat="identity") +
            scale_fill_viridis_d() +
            scale_color_viridis_d() +
            geom_text(data, 
                      mapping = aes(x=factor(array_size), 
                                    y = mp99 + p99sd,
                                    label = mp99,
                                    family = "Fira Sans",
                                    vjust = -0.45,
                                    hjust = -0.1,
                                    angle = 45), 
                      size = 2,
                      position = position_dodge2(width = 0.9, preserve = "single")) +
            labs(x = "Working Set Size (bytes)", y = "p99 Latency (µs)")

    print(plot)
    return(plot)
}

base_tput_plot <- function(data) {
    plot <- ggplot(data,
                   aes(x = factor(array_size),
                       y = tputkneepps,
                       fill = system_name)) +
                expand_limits(y = 0) +
            geom_bar(position="dodge", stat="identity", width = 0.9) +
            geom_errorbar(aes(ymin=tputkneepps - tputkneeppssd, ymax=tputkneepps + tputkneeppssd),position="dodge", stat="identity") +
            scale_fill_viridis_d() +
            scale_color_viridis_d() +
            geom_text(data, 
                      mapping = aes(x=factor(array_size), 
                                    y = tputkneepps + tputkneeppssd,
                                    label = tputkneepps,
                                    family = "Fira Sans",
                                    vjust = -0.45,
                                    hjust = -0.1,
                                    angle = 45), 
                      size = 2,
                      position = position_dodge2(width = 0.9, preserve = "single")) +
            labs(x = "Working Set Size (bytes)", y = "Highest Offered Load under 25 µs (Requests / sec)")

}

label_plot <- function(plot) {
    plot <- plot +
            theme_light() +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans"),
                  legend.title = element_blank(),
                  legend.key.size = unit(10, 'mm'),
                  legend.spacing.x = unit(0.1, 'cm'),
                  legend.text=element_text(size=15),
                  axis.title=element_text(size=27,face="plain", colour="#000000"),
                  axis.text.y=element_text(size=10, colour="#000000"),
                  axis.text.x=element_text(size=10, colour="#000000", angle=45))
}

base_p99_tput_latency <- function(data, y_cutoff) {
    print(data)
    plot <- ggplot(data,
                    aes(x = offered_load_pps,
                        y = mp99,
                        color = system_name,
                        shape = system_name,
                        ymin = mp99 - p99sd,
                        ymax = mp99 + p99sd)) +
                coord_cartesian(ylim = c(0, y_cutoff), expand = FALSE) +
    labs(x = "Throughput (Million Requests/sec)", y = "p99 Latency (µs)")
    return(plot)
}

base_median_tput_latency <- function(data, y_cutoff) {
    print(data)
    plot <- ggplot(data,
                    aes(x = offered_load_pps,
                        y = mmedian,
                        color = system_name,
                        shape = system_name,
                        ymin = mmedian - mediansd,
                        ymax = mmedian + mediansd)) +
                coord_cartesian(ylim = c(0, y_cutoff), expand = FALSE) +
    labs(x = "Throughput (Requests/sec)", y = "Median Latency (µs)")
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

base_plot <- function(data, metric) {
    if (metric == "p99") {
        base_plot <- base_p99_plot(data)
        base_plot <- label_plot(base_plot)
        return(base_plot)
    } else if (metric == "median") {
        base_plot <- base_median_plot(data)
        base_plot <- label_plot(base_plot)
        return(base_plot)
    } else if (metric == "tput") {
        base_plot <- base_tput_plot(data)
        base_plot <- label_plot(base_plot)
        return(base_plot)
    }
}

individual_plot <- function(data, metric, size, values) {
    data <- subset(data, num_segments == values & total_size == size)
    print(data)
    plot <- base_plot(data, metric)
    print(plot)
    return(plot)
}

tput_latency_plot <- function(data, metric, size, values, arrays) {
    data <- subset(data, num_segments == values & total_size == size & array_size == arrays)
    plot <- base_tput_latency_plot(data, metric)
    print(plot)
    return(plot)
}
full_plot <- function(data, metric) {
    plot <- base_plot(data, metric) +
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

d$system_name <- apply(d, 1, get_system_name)
d$total_size <- d$segment_size * d$num_segments
summarized <- ddply(d, c("system_name", "segment_size", "num_segments", "with_copy", "as_one", "total_size", "array_size", "offered_load_pps", "offered_load_gbps"),
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
    plot <- full_plot(d_postprocess, metric)
    ggsave("tmp.pdf", width=8, height=11)
    embed_fonts("tmp.pdf", outfile=plot_pdf)

} else if (plot_type == "individual") {
    total_size <- strtoi(args[6])
    num_segments <- strtoi(args[7])
    plot <- individual_plot(d_postprocess, metric, total_size, num_segments)
    ggsave("tmp.pdf", width=9, height=6)
    embed_fonts("tmp.pdf", outfile=plot_pdf)
} else if (plot_type == "tput_latency_arraysize") {
    total_size <- strtoi(args[6])
    num_segments <- strtoi(args[7])
    array_size <- strtoi(args[8])
    plot <- tput_latency_plot(summarized, metric, total_size, num_segments, array_size)
    ggsave("tmp.pdf", width = 9, height=6)
    embed_fonts("tmp.pdf", outfile=plot_pdf)
} else if (plot_type == "tput_latency_cpucyles") {
}




