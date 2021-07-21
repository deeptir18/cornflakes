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
# d <- subset(d, d[9] * 100 > 88)

labels <- c("cf_zero_copy_send_zero_copy_recv" = "Cornflakes, Zero-Copy Send and Zero-Copy  Receive", "cf_copy_send_zero_copy_recv" = "Cornflakes, Copy-Send and Zero-Copy Receive", "flatbuffers" = "Flatbuffers", "capnproto" = "Capnproto", "protobuf" = "Protobuf")

shape_values <- c('cf_zero_copy_send_zero_copy_recv' = 7, 'cf_copy_recv_copy_send' = 4, 'cf_zero_copy_send_zero_copy_recv' = 18, 'cf_copy_send_zero_copy_recv' = 17)
color_values <- c('cf_zero_copy_send_zero_copy_recv' = '#1b9e77',
                    'cf_copy_send_zero_copy_recv' = '#d95f02',
                    'cf_copy_recv_copy_send' = '#7570b3',
                    'cf_zero_copy_send_copy_recv' = '#e7298a')
shape_values <- c('protobuf' = 4, 'capnproto' = 18, 'flatbuffers' = 17, 'cf_copy_send_zero_copy_recv' = 15, 'cf_zero_copy_send_zero_copy_recv' = 19)
color_values <- c('cf_zero_copy_send_zero_copy_recv' = '#1b9e77',
                    'cf_copy_send_zero_copy_recv' = '#d95f02',
                    'flatbuffers' = '#7570b3',
                    'capnproto' = '#e7298a',
                    'protobuf' = '#e6ab02')
#line_types <- c('baseline_zero_copy' = 'solid', 'baseline' = 'solid', 'capnproto' = 'solid', 'flatbuffers' = 'solid', 'protobtyes' = 'solid', 'protobuf' = 'solid', 'cornflakes' = 'dashed')

# add in the row name based on the zero-copy flags
get_system_name <- function(row) {
    if (row["recv_mode"] == "zero_copy_recv" && row["serialization"] == "cornflakes-dynamic") {
        res <- "cf_zero_copy_send_zero_copy_recv"
    }
    else if (row["recv_mode"] == "zero_copy_recv" && row["serialization"] == "cornflakes1c-dynamic") {
        res <- "cf_copy_send_zero_copy_recv"
    } else {
    #else if (row["recv_mode"] == "copy_to_dma_memory" && row["serialization"] == "cornflakes-dynamic") {
    #    res <- "cf_zero_copy_send_copy_recv"
    #}
    #else if (row["recv_mode"] == "copy_out_recv" && row["serialization"] == "cornflakes1c-dynamic") {
    #    res <- "cf_copy_recv_copy_send"
        res <- row["serialization"]
    }
    return(res)
}
options(width=10000)

d$system_name <- apply(d, 1, get_system_name)
d <- subset(d, d$system_name != "cf_zero_copy_send_copy_recv")
summarized <- ddply(d, c("system_name", "size", "message_type", "achieved_load_gbps"),
                    summarise,
                    mavg = mean(avg),
                    mp99 = mean(p99),
                    mp999 = mean(p999),
                    avgmedian = mean(median))
print(summarized)
gathered <- gather(summarized, key="latency", value = "mmt", -system_name, -size, -message_type, -achieved_load_gbps)
gathered_p99 <- subset(gathered, gathered$latency == "avgmedian")
print(gathered_p99)

base_plot <- function(data) {
    plot <- ggplot(data,
                   aes(x = achieved_load_gbps,
                       y = mmt,
                       color = system_name,
                       shape = system_name,
                       )) +
            expand_limits(x = 0, y = 0) +
            geom_point(size=2) +
            geom_line(size = 1, aes(color=system_name)) +
            labs(x = "Throughput (Requests/ms)", y = "Latency (µs)") +
            scale_shape_manual(values = shape_values, labels = labels) +
            scale_color_manual(values = color_values ,labels = labels) +
            scale_fill_manual(values = color_values, labels = labels) +
            theme_light() +
            theme(legend.position = "top",
                  text = element_text(family="Fira Sans"),
                  legend.title = element_blank(),
                  legend.key.size = unit(10, 'mm'),
                  legend.spacing.x = unit(0.1, 'cm')) +
    coord_cartesian(ylim=c(0, 50))
    return(plot)
}

full_plot <- function(data) {
    plot <- base_plot(data)
    plot <- plot +
        labs(x = "Achieved Throughput (Gbps)", y = "Median Latency (µs)") +
        facet_grid(message_type ~ size)
    print(plot)
    return(plot)
}

full_plot(gathered_p99)
ggsave("tmp.pdf", width=9, height=9)
embed_fonts("tmp.pdf", outfile=args[2])


