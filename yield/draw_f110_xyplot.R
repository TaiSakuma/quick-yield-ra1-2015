#!/usr/bin/env Rscript
# Tai Sakuma <tai.sakuma@cern.ch>

##__________________________________________________________________||
argv <- commandArgs(trailingOnly = FALSE)
scriptdir = dirname(dirname(substring(argv[grep("--file=", argv)], 8)))
olddir <- setwd(file.path(scriptdir, 'mianRs'))
source('drawFuncs.R')
source('drawThemes.R')
source('custom_read_table.R')
source('lib.R')
source('all_outputs_are_newer_than_any_input.R')
setwd(olddir)

##__________________________________________________________________||
thisdir = dirname(substring(argv[grep("--file=", argv)], 8))
olddir <- setwd(thisdir)
source('panels.R')
setwd(olddir)

##__________________________________________________________________||
library(latticelog)
library(tidyr, warn.conflicts = FALSE, quietly = TRUE)
library(dplyr, warn.conflicts = FALSE, quietly = TRUE)

library(stringr)

##__________________________________________________________________||
theme.this <- function()
  {
    cols <- c("darkgreen", "orange", "#ff00ff", "#ff0000", "#0080ff", "#00ff00", "brown")
    col.regions <- colorRampPalette(brewer.pal(9, "Blues"))(100)
    cols <- brewer.pal(8, "Dark2")
    theme <- list(
      add.text = list(cex = 0.8, lineheight = 2.0), # text in strip
      axis.text = list(cex = 0.8),
      axis.line = list(lwd = 0.2, col = 'gray30'),
      reference.line = list(col = '#eeeeee', lwd = 0.2),
      regions = list(col = col.regions),
      superpose.line = list(col = cols, lwd = 1.5, alpha = 1),
      background = list(col = "transparent")
    )
    modifyList(theme.economist(), theme)
  }

##__________________________________________________________________||
eval(readArgs)

##__________________________________________________________________||
arg.tbl.dir <- if(exists('arg.tbl.dir')) arg.tbl.dir else 'tbl_01'

##__________________________________________________________________||
main <- function() {


  varname_list <- list('ht', 'mht', 'njet', 'nb')

  tbl_list <- vector('list', length(varname_list))

  for(i in seq_along(varname_list)) {
    varname <- varname_list[[i]]
    tbl_var_01 = custom_read_table(file.path(arg.tbl.dir, str_c('tbl_01_n.component.smsmass1.smsmass2', varname, 'txt', sep = '.')))
    tbl_var_02 = custom_read_table(file.path(arg.tbl.dir, str_c('tbl_02_n.component.smsmass1.smsmass2', varname, 'txt', sep = '.')))
    tbl_var_01$selection <- as.integer(0)
    tbl_var_02$selection <- as.integer(1)
    tbl_var <- bind_rows(tbl_var_01, tbl_var_02)
    tbl_var$var <- varname
    names(tbl_var)[names(tbl_var) == varname] <- 'val'
    tbl_list[[i]] <- tbl_var
  }
  tbl <- bind_rows(tbl_list)

  tbl <- tbl[c('component', 'smsmass1', 'smsmass2', 'selection', 'var', 'val', 'n')]
  call.write.table.aliened(tbl, 'd.txt')

  tbl <- tbl[tbl$val <= 5000, ]
  tbl <- tbl[tbl$n > 0, ]

  fig.id <- mk.fig.id()

  figFileNameNoSuf <- paste(fig.id, sep = '_')
  suffixes <- c('.pdf', '.png')
  figFileName <- outer(figFileNameNoSuf, suffixes, paste, sep = '')
  figPaths <- file.path(arg.outdir, figFileName)

  dir.create(arg.outdir, recursive = TRUE, showWarnings = FALSE)

  theme <- theme.this()

  print(tbl)

  p <- draw_figure(tbl)

  if(length(dim(p)) == 2) {
    p <- useOuterStrips(p)
  }

  layout.aspect <- 1

  if (length(dim(p)) == 1 && is.null(p$layout)) {
    # need to determine the layout
    layout.aspect <- layout.aspect
    if(is.null(layout.aspect)) layout.aspect <- 1
    npanels <- sum(dim(p))
    nrows <- ceiling(sqrt(layout.aspect*npanels))
    ncols <- ceiling(npanels/nrows)
    layout <- c(ncols, nrows)
    p$layout <- layout
  }

  call_print_figure_autosize(
    p, fig.id = figFileNameNoSuf, theme = theme,
    width_per_panel = 2,
    extra_width = 1,
    extra_height = 1
  )

  invisible()
}

##__________________________________________________________________||
panel <- function(x, y, subscripts, groups = NULL, ...)
{
  lim <- current.panel.limits()
  if(length(x) == 0) return()
  panel.grid(h = -1, v = -1)
  panel.xyplot(x, y, subscripts = subscripts, groups = groups, type = 's', ...)
}

##__________________________________________________________________||
draw_figure <- function(tbl)
{

  golden_ratio <- 1.61803398875
  xyplot(log10(n) ~ val | var,
         group = selection,
         tbl,
         xlab = NULL,
         aspect = 1/golden_ratio,
         between = list(x = 0.2, y = 0.2),
         scales = list(
           x = list(alternating = '1', relation = 'free'),
           y = list(alternating = '1', relation = 'free')
         ),
         panel = panel
         )
}

##__________________________________________________________________||
main()
