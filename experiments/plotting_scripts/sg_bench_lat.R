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

calculate_difference <- function(row) {
    res <- (row["scatter_gather"] - row["copy_whole_segment"]) / row["copy_each_segment"]
    return(res)
}

WIDTH <- 0.9

d$system_name <- apply(d, 1, get_system_name)

## summary statistics
summarized <- ddply(d, c("system_name", "segment_size", "num_mbufs"),
                    summarise,
                    # mavg = mean(avg),
                    mmedian = mean(median),
                    # mp99 = mean(p99))
                    # p99sd = sd(p99),
                    # mp999 = mean(p999))
                    # moffered_load_pps = mean(offered_load_pps),
                    # moffered_load_gbps = mean(offered_load_gbps),
                    # machieved_load_pps = mean(achieved_load_pps),
                    # machieved_load_gbps = mean(achieved_load_gbps))

## make a re-shaped version of the data
heatmap_data <- summarized %>% spread(key = system_name, value = mmedian)
## make a new row based on % better / % difference
heatmap_data$difference <- apply(heatmap_data, 1, calculate_difference)

normalize <- function(x, min_val, max_val) {

return ((x["difference"] - min_val) / (max_val - min_val))
}

calculate_x_bin <- function(x) {
    seg_size <- x["segment_size"]
    if (seg_size == 64) {
        res <- 0
    }
    else if (seg_size == 128) {
        res <- 1
    }
    else {
        res <- ( 2 + (seg_size - 256)/256)
    }
    return(res)
}


min_value  <- min(heatmap_data[,"difference"])
max_value  <- max(heatmap_data[,"difference"])
heatmap_data$norm_difference <- apply(heatmap_data, 1, normalize, min_value, max_value)

heatmap_subset <- subset(heatmap_data, num_mbufs == 8)
print(heatmap_subset)

ggplot(heatmap_data,
            aes(x = factor(segment_size),
                y = num_mbufs,
                fill = difference)) +
            coord_fixed(ratio = 1) +
            geom_tile() +
            xlab(label = "Segment Size") +
            ylab(label = "number of mbufs") +
            scale_fill_gradientn(colours=c("#f1a340", "#f7f7f7", "#998ec3")) +
            # scale_fill_viridis_c(option="magma") +
            expand_limits(x = 4000, y = 1) +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans"),
                  legend.title = element_blank(),
                  legend.key.size = unit(10, 'mm'),
                  legend.spacing.x = unit(0.1, 'cm'),
                  plot.margin = unit(c(0, 0, 0, 0), "cm"),
                  legend.text=element_text(size=15),
                  axis.title=element_text(size=27,face="plain", colour="#000000"),
                  axis.ticks.y = element_blank(),
                  axis.ticks.x= element_blank(),
                  axis.text.y=element_text(size=27, colour="#000000"),
                  axis.text.x=element_text(size=10, colour="#000000", angle=45)) +
            ylim(1,32)
ggsave("tmp.pdf", width=9, height=9)
embed_fonts("tmp.pdf", outfile=args[2])
            

