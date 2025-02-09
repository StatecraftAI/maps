# Populist Consensus Website

Welcome to the open source repo for [The Populist Consensus](https://www.populistconsensus.com).

## Build

Assuming Linux. Install Ruby and deps follwing the [instructions here](https://jekyllrb.com/docs/).

```bash
gem install bundler jekyll
bundle clean --force
gem install bundler jekyll
bundle update
bundle install
rm -rf node_modules package-lock.json
bundle exec jekyll serve --watch --trace --drafts --verbose
```

## Credits

- [Github Pages](https://pages.github.com) for hosting.
- [Start Bootstrap](http://startbootstrap.com) for the site template.
- [unsplash](http://unsplash.com) for stock photos.
- [Font Awesome](http://fortawesome.github.io/Font-Awesome) for the CSS toolkit.

## License

This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](LICENSE).

![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png "license")

## Disclaimer

Please review the [Disclaimer](DISCLAIMER).
