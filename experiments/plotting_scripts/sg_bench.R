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
plot_pdf <- args[2]
metric <- args[3]
plot_type <- args[4]
# argument 5: if individual -- total size
# argument 6: if individual -- num segments

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
            labs(x = "Working Set Size (bytes)", y = "Median Latency (ns)")
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
            labs(x = "Working Set Size (bytes)", y = "p99 Latency (ns)")
    print(plot)
    return(plot)
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

base_plot <- function(data, metric) {
    if (metric == "p99") {
        base_plot <- base_p99_plot(data)
        base_plot <- label_plot(base_plot)
        return(base_plot)
    } else if (metric == "median") {
        base_plot <- base_median_plot(data)
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

full_plot <- function(data, metric) {
    plot <- base_plot(data, metric) +
        facet_grid(total_size ~ num_segments) +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans"),
                  legend.title = element_blank(),
                  legend.key.size = unit(10, 'mm'),
                  legend.spacing.x = unit(0.1, 'cm'),
                  axis.title=element_text(size=27,face="plain", colour="#000000"),
                  axis.text=element_text(size=4, colour="#000000")) +
        ylim(0,12000)

    print(plot)
    return(plot)
        
}

d$system_name <- apply(d, 1, get_system_name)
d$total_size <- d$segment_size * d$num_segments
summarized <- ddply(d, c("system_name", "segment_size", "num_segments", "with_copy", "as_one", "total_size", "array_size"),
                    summarise,
                    mavg = mean(avg),
                    mmedian = mean(median),
                    mediansd = sd(median),
                    mp99 = mean(p99),
                    p99sd = sd(p99),
                    mp999 = mean(p999),
                    moffered_load_pps = mean(offered_load_pps),
                    moffered_load_gbps = mean(offered_load_gbps),
                    machieved_load_pps = mean(achieved_load_pps),
                    machieved_load_gbps = mean(achieved_load_gbps))

if (plot_type == "full") {
    plot <- full_plot(summarized, metric)
    ggsave("tmp.pdf", width=8, height=11)
    embed_fonts("tmp.pdf", outfile=plot_pdf)

} else if (plot_type == "individual") {
    total_size <- strtoi(args[5])
    num_segments <- strtoi(args[6])
    plot <- individual_plot(summarized, metric, total_size, num_segments)
    ggsave("tmp.pdf", width=9, height=6)
    embed_fonts("tmp.pdf", outfile=plot_pdf)
} 




