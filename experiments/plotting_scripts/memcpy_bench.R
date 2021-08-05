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
subset_arg <- strtoi(args[2])
plot_file <- args[3]

# add in the row name
get_system_name <- function(row) {
    if (row["with_copy"] == "True") {
        res <- "Memcpy"
    } else if (row["with_copy"] == "False") {
        res <- "Read"
    }
    return(res)
}
WIDTH <- 0.9

d$system_name <- apply(d, 1, get_system_name)
summarized <- ddply(d, c("system_name", "segment_size", "with_copy", "array_size"),
                    summarise,
                    mavg = mean(time),
                    avgsd = sd(time),
                    medianavg = median(time))

array_size_plot <- function(data) {
    data <- subset(data, data$segment_size == subset_arg)
    plot <- ggplot(data, aes(x = factor(array_size), y = mavg, fill = system_name)) +
	expand_limits(y = 0) +
	           geom_bar(position="dodge", stat="identity", width = 0.9) +
            geom_errorbar(aes(ymin=mavg-avgsd, ymax=mavg+avgsd),position="dodge", stat="identity") +
            scale_fill_viridis_d() +
            scale_color_viridis_d() +
            geom_text(data, 
                      mapping = aes(x=factor(array_size), 
                                    y = mavg + avgsd, 
                                    label = mavg,
                                    family = "Fira Sans",
                                    vjust = -1.0), 
                      position = position_dodge2(width = 0.9, preserve = "single")) +
            labs(x = "Array Size (bytes)", y = "Average Latency (ns)") +
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

if (args[4] == "by_array_size") {
    array_size_plot(summarized)
}
ggsave("tmp.pdf", width=9, height=6)
embed_fonts("tmp.pdf", outfile=args[3])

