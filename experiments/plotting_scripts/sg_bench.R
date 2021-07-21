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
segment_size_amt <- strtoi(args[2])
plot_file <- args[3]

labels <- c("scatter_gather" = "Scatter Gather", "copy_each_segment" = "Copy Individual Segments", "copy_whole_segment" = "Copy As One Buffer")

# add in the row name
get_system_name <- function(row) {
    if (row["with_copy"] == "False" && row["as_one"] == "False") {
        res <- "scatter_gather"
    }
    else if (row["with_copy"] == "True" && row["as_one"] == "False") {
        res <- "copy_each_segment"
    }
    else if (row["with_copy"] == "True" && row["as_one"] == "True") {
        res <- "copy_whole_segment"
    }
    return(res)
}
WIDTH <- 0.9

d$system_name <- apply(d, 1, get_system_name)
d <- subset(d, segment_size == segment_size_amt)
summarized <- ddply(d, c("system_name", "segment_size", "num_mbufs", "with_copy", "as_one"),
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

base_plot <- function(data) {
    plot <- ggplot(data,
                   aes(x = factor(num_mbufs),
                       y = mmedian,
                       fill = system_name)) +
            expand_limits(y = 0) +
            geom_bar(position="dodge", stat="identity", width = 0.9) +
            geom_errorbar(aes(ymin=mmedian-mediansd, ymax=mmedian+mediansd),position="dodge", stat="identity") +
            scale_fill_viridis_d() +
            scale_color_viridis_d() +
            geom_text(data, 
                      mapping = aes(x=factor(num_mbufs), 
                                    y = mmedian + mediansd, 
                                    label = mmedian,
                                    family = "Fira Sans",
                                    vjust = -1.0), 
                      position = position_dodge2(width = 0.9, preserve = "single")) +
            labs(x = "Number of Segments", y = "Median Latency (ns)") +
                        theme_light() +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans"),
                  legend.title = element_blank(),
                  legend.key.size = unit(10, 'mm'),
                  legend.spacing.x = unit(0.1, 'cm'),
                  legend.text=element_text(size=15),
                  axis.title=element_text(size=27,face="plain", colour="#000000"),
                  axis.text.y=element_text(size=27, colour="#000000"),
                  axis.text.x=element_text(size=10, colour="#000000", angle=45))
    print(plot)
    return(plot)
}

base_plot(summarized)
ggsave("tmp.pdf", width=9, height=6)
embed_fonts("tmp.pdf", outfile=args[3])



