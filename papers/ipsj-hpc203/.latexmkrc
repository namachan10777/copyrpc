#!/usr/bin/env perl

# Use uplatex
$latex = 'uplatex -synctex=1 -halt-on-error -interaction=nonstopmode %O %S';
$pdflatex = 'pdflatex -synctex=1 -halt-on-error -interaction=nonstopmode %O %S';
$lualatex = 'lualatex -synctex=1 -halt-on-error -interaction=nonstopmode %O %S';
$xelatex = 'xelatex -synctex=1 -halt-on-error -interaction=nonstopmode %O %S';

# Use dvipdfmx
$dvipdf = 'dvipdfmx %O -o %D %S';
$dvips = 'dvips %O -z -f %S | convbg > %D';
$ps2pdf = 'ps2pdf %O %S %D';

# BibTeX
$bibtex = 'upbibtex %O %B';
$biber = 'biber --bblencoding=utf8 -u -U --output_safechars %O %B';

# Index
$makeindex = 'upmendex %O -o %D %S';

# PDF mode: 3 = dvipdfmx
$pdf_mode = 3;

# Clean extensions
$clean_ext = 'synctex.gz synctex.gz(busy) run.xml tex.bak bbl bcf fdb_latexmk nav snm';
