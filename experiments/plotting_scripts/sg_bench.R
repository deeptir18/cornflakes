#!/usr/bin/env Rscript

library(ggplot2)
library(plyr)
library(tidyr)
library(extrafont)
library(showtext)
library("stringr")  
library(viridis)
font_add_google("Fira Sans")
showtext_auto()

x_labels_arrays <- c("65536" = "65536\nIn L1", "819200" = "819200\nIn L2", "4096000" = "4096K\nIn L3", "65536000" = "65536K\n~4X L3", "655360000" = "655360K\n~40x L3")
x_labels_recv_size <- c("256" = "256", "512" = "512", "1024" = "1024", "2048" = "2048", "4096" = "4096")

x_labels_total_size <- c("256" = "256", "512" = "512", "1024" = "1024", "2048" = "2048", "4096" = "4096", "8192" = "8192")
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
        return(args[8])
        #return("Number of Segments")
    } else if (factor == "total_size") {
        return("Total Packet Size (Bytes)")
    }
    return(factor)
}

# shape_values <- c('zero_copy' = 19, 'copy' = 15, 'zero_copy_refcnt' = 18)

#color_values <- c('zero_copy' = '#1b9e77', 
  #                  'copy' = '#d95f02',
 #                   'zero_copy_refcnt' = '#7570b3' )
#levels <- c('zero_copy', 'zero_copy_refcnt', 'copy')

shape_values <- c('zero_copy' = 19, 'copy' = 15)

color_values <- c('zero_copy' = '#1b9e77', 
                  'copy' = '#d95f02')
levels <- c('zero_copy', 'copy')


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
d_postprocess$system <- factor(d_postprocess$system, levels = levels)
d_postprocess$total_size = d_postprocess$segment_size * d_postprocess$num_segments

# convert to us
d$p99 <- d$p99 / 1000.0
d$median <- d$median / 1000.0
d$offered_load_pps <- d$offered_load_pps / 1000000.0
d$achieved_load_pps <- d$achieved_load_pps / 1000000.0

#labels <- c("zero_copy" = "SG with 0 Cache Misses", "copy" = "Copy with Data Cache Misses", 
#"zero_copy_refcnt" = "SG with 2 Cache Misses")
labels <- c("zero_copy" = "Scatter Gather", "copy" = "Copy Each Segment")

base_median_plot <- function(data, factor_name, x_axis_name, x_axis_labels) {
    factor_name_string <- sprintf("factor(%s)", factor_name)
    plot <- ggplot(data,
                  aes_string(x = factor_name_string,
                      y = "mmedian",
                      fill  = "system")) +
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
                      fill  = "system")) +
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
    print(data)
    factor_name_string <- sprintf("factor(%s)", factor_name)
    x_name = sprintf("reorder(ds_shape_name, %s)", factor_name)
    y_name <- "maxtputpps"
    y_label <- "round(maxtputpps, 2)"
    y_label_height <- "maxtputpps + maxtputppssd"
    y_axis <- "Highest Achieved Load\n(100K Requests / sec)"
    if (metric == "tput_gbps") {
        y_name <- "maxtputgbps"
        y_label <- "round(maxtputgbps, 1)"
        y_label_height <- "maxtputgbps + 7"
        y_axis <- "Highest Achieved\nThroughput (Gbps)"
    }
    print(color_values)
    plot <- ggplot(data,
                   aes_string(x = x_name,
                       y = y_name,
                       fill = "system",
                       shape = "system",
                       group = "system",
                       color = "system")) +
                expand_limits(y = 0) +
            geom_line(size = 1) +
            #geom_bar(position=position_dodge(0.7), stat="identity", width = 0.05) +
            geom_point(size = 2.75) +
            #geom_text(position = position_dodge(0.7),
            #        aes_string(y=y_label_height, label = y_label),
            #       size = 2.75,
            #       angle = 70, colour = "black") +
            scale_shape_manual(values = shape_values, labels = labels, breaks = levels) +
            scale_color_manual(values = color_values, labels = labels, breaks = levels) +
            scale_fill_manual(values = color_values, labels = labels, breaks = levels) +
            scale_x_discrete(labels = function(x) str_wrap(x, width = 12)) +
            # add space for the labels
            scale_y_continuous(expand = expansion(mult = c(0, .15))) +
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
                  legend.text=element_text(size=11),
                  axis.title=element_text(size=11,face="plain", colour="#000000"),
                  axis.text.y=element_text(size=11, colour="#000000"),
                  axis.text.x=element_text(size=8, colour="#000000", angle=0)) +
                guides(colour = guide_legend(nrow = 1, byrow = TRUE),
                       fill = guide_legend(nrow = 1, byrow = TRUE),
                       shape = guide_legend(nrow = 1, byrow = TRUE))
}

base_p99_tput_latency <- function(data, y_cutoff) {
    # print(data)
    plot <- ggplot(data,
                    aes(x = machieved_load_pps,
                        y = mp99,
                        color = system,
                        shape = system,
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
                        color = system,
                        shape = system,
                        ymin = mmedian - mediansd,
                        ymax = mmedian + mediansd)) +
                coord_cartesian(ylim = c(0, y_cutoff), expand = FALSE) +
    labs(x = "Achieved Load (100K Requests/sec)", y = "Median Latency (µs)")
    return(plot)

}

label_tput_latency <- function(plot) {
    plot <- plot +
            geom_point(size=4) +
            geom_line(size = 1, aes(color=system)) +
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
        x_axis_name <- x_axis_name_func(factor_name)
        if (factor_name == "num_segments") {
            x_axis_name = args[8]
        }
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

label_heatmap <- function(row) {
    scatter_gather <- round(row["zero_copy"], digits = 2)
    copy <- round(row["copy"], digits = 2)
    difference <- round(row["difference"], digits = 2)
    label <- paste("SG: ", scatter_gather)
    label <- paste(label, "\n")
    label <- paste(label, "Copy: ")
    label <- paste(label, copy)
    label <- paste(label, "\n(")
    if (difference > 0) {
        label <- paste(label, "+")
    }
    label <- paste(label, difference*100)
    label <- paste(label, "%)")
    return (label)

}

calculate_latency_difference <- function(row) {
    res <- (row["copy"] - row["zero_copy"]) / row["zero_copy"]
    return (res)
}

calculate_tput_difference <- function(row) {
    res <- (row["zero_copy"] - row["copy"]) / row["copy"]
    return (res)
}
normalize <- function(x, min_val, max_val) {
    return ((x["difference"] - min_val) / (max_val - min_val))
}

system_heatmap <- function(data, metric, system_name) {
    # TODO: finish system heatmap plot
    subset <- ddply(data, c("system", "num_segments", "total_size"), summarise, tput = median(maxtputgbps))
    subset <- subset[subset$system == system_name]
    heatmap_data <- subset %>% spread(key = system, value = tput)
    heatmap_data$difference <- apply(heatmap_data, 1, calculate_tput_difference)

}
heatmap_plot <- function(data, metric) {
    subset <- ddply(data, c("system", "num_segments", "total_size"), summarise, tput = mean(maxtputgbps))
    heatmap_data <- subset %>% spread(key = system, value = tput)
    heatmap_data$difference <- apply(heatmap_data, 1, calculate_tput_difference)
    if (metric == "p99") {
        subset <- ddply(data, c("system", "num_segments", "total_size"), summarise, tput = mean(mp99))
        heatmap_data <- subset %>% spread(key = system, value = p99)
        heatmap_data$difference <- apply(heatmap_data, 1, calculate_latency_difference)
    } else if (metric == "median") {
        subset <- ddply(data, c("system", "num_segments", "total_size"), summarise, tput = mean(mmedian))
        heatmap_data <- subset %>% spread(key = system, value = median)
        heatmap_data$difference <- apply(heatmap_data, 1, calculate_latency_difference)
    }
    min_value  <- min(heatmap_data[,"difference"])
    max_value  <- max(heatmap_data[,"difference"])
    heatmap_data$norm_difference <- apply(heatmap_data, 1, normalize, min_value, max_value)
    heatmap_data$label <- apply(heatmap_data, 1, label_heatmap)

    plot<-ggplot(heatmap_data,
        aes(x = factor(total_size),
            y = factor(num_segments),
            fill = difference,
            )) +
        coord_fixed(ratio = 1) +
        geom_tile() +
        xlab(label = "Total Request Size (bytes)") +
        ylab(label = "Number of Segments Requested") + 
        geom_text(aes(label=label, family = "Fira Sans")) +
        scale_fill_gradient2(low = "#f1a340", mid = "#f7f7f7", high = "#998ec3") +
        expand_limits(x = 0, y = 1) +
        theme_bw() +
        theme(legend.position = "top",
                text = element_text(family="Fira Sans"),
                legend.title = element_blank(),
                legend.key.size = unit(10, 'mm'),
                legend.spacing.x = unit(0.1, 'cm'),
                panel.grid.major = element_blank(), 
                panel.border = element_blank(),
                panel.grid.minor = element_blank(),
                # plot.margin = unit(c(0, 0, 0, 0), "cm"),
                legend.text=element_text(size=15),
                axis.title=element_text(size=27,face="plain", colour="#000000"),
                axis.ticks.y = element_blank(),
                axis.ticks.x= element_blank(),
                axis.text.y=element_text(size=27, colour="#000000"),
                axis.text.x=element_text(size=27, colour="#000000", angle = 23))
    print(plot)
    return(plot)
}

if (!("recv_size" %in% colnames(d)))
{
    d$recv_size <- 0
}

d$total_size <- d$segment_size * d$num_segments
summarized <- ddply(d, c("system", "segment_size", "num_segments", "total_size", "array_size", "recv_pkt_size", "busy_cycles", "offered_load_pps", "offered_load_gbps"),
                    summarise,
                    mavg = mean(avg),
                    mmedian = mean(median),
                    mediansd = sd(median),
                    mp99 = mean(p99),
                    p99sd = sd(p99),
                    mp999 = mean(p999),
                    machieved_load_pps = mean(achieved_load_pps),
                    machieved_load_gbps = mean(achieved_load_gbps))
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
        ggsave("tmp.pdf", width=5, height=2)
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
} else if (plot_type == "heatmap") {
    plot <- heatmap_plot(d_postprocess, metric)
    ggsave("tmp.pdf", width=9, height=9)
    embed_fonts("tmp.pdf", outfile=plot_pdf)
} else if (plot_type == "system_heatmap") {
    system <- strtoi(args[6])
    plot <- heatmap_plot(d_postprocess, metric, system)
    ggsave("tmp.pdf", width=9, height=9)
    embed_fonts("tmp.pdf", outfile=plot_pdf)
}
