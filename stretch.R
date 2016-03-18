library(ggplot2)

input_file <- 'stretch-1/stretch-1-1-300.csv'

data <- read.table(input_file, header=TRUE)

plot <- ggplot(data, aes(x = JOB_NUMBER))

print(
  plot +
    #geom_point(aes(y=BSLD)) +
    geom_line(aes(y=AVG_BSLD))
)

ggsave(output_file)
