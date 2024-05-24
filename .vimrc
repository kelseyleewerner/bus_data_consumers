"Shows line numbers in all files
set number

"Maps esc key to jj
inoremap jj <ESC>

"Removes trailling white spaces
fun! <SID>StripTrailingWhitespaces()
  let l = line(".")
  let c = col(".")
  %s/\s\+$//e
  call cursor(l, c)
endfun
autocmd FileType c,cpp,java,php,ruby,python autocmd
	\ BufWritePre <buffer> :call <SID>StripTrailingWhitespaces()
autocmd BufWritePre Makefile :call <SID>StripTrailingWhitespaces()

"Converts tabs to spaces
:set smartindent
set expandtab autoindent shiftwidth=2 tabstop=2
