# Tai Sakuma <tai.sakuma@cern.ch>


##__________________________________________________________________||
call_print_figure_autosize <- function(p, fig.id, theme, width_per_panel = 1,
                                       extra_width = 2.6, extra_height = 1.0) {

  height_per_panel <- width_per_panel*p$aspect.ratio
  width <- p$layout[1]*width_per_panel + extra_width
  height <- p$layout[2]*height_per_panel + extra_height

  dir.create(arg.outdir, recursive = TRUE, showWarnings = FALSE)
  print.figure(p, fig.id = fig.id, theme = theme, width = width, height = height)

}

##__________________________________________________________________||
