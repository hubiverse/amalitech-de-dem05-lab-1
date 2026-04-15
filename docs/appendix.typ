= Appendix <appendix>

== Appendix A: Highest Revenue <appendix-highest-revenue>


#figure(
  table(
    columns: (2fr, 1fr, 1fr, 1fr),
    inset: 10pt,
    align: (x, y) => if x == 0 { left + horizon } else { center + horizon },
    fill: (x, y) => if y == 0 { gray.lighten(60%) },
    [* Title *], [* Revenue Musd *], [* Vote Count *], [* Budget Musd *],
    [Avatar], [2,923.71], [33,763.00], [237.00],
    [Avengers: Endgame], [2,799.44], [27,486.00], [356.00],
    [Titanic], [2,264.16], [26,987.00], [200.00],
    [Star Wars: The Force Awakens], [2,068.22], [20,423.00], [245.00],
    [Avengers: Infinity War], [2,052.41], [31,722.00], [300.00],
  ),
  caption: [Top 5 Highest Revenue Movies],
) <tab:top_revenue>

 == Appendix B: Highest Budget <appendix-highest-budget>


#figure(
  table(
    columns: (2fr, 1fr, 1fr, 1fr),
    inset: 10pt,
    align: (x, y) => if x == 0 { left + horizon } else { center + horizon },
    fill: (x, y) => if y == 0 { gray.lighten(60%) },
    [* Title *], [* Budget Musd *], [* Vote Count *], [* Revenue Musd *],
    [Avengers: Endgame], [356.00], [27,486.00], [2,799.44],
    [Star Wars: The Last Jedi], [300.00], [16,221.00], [1,332.70],
    [Avengers: Infinity War], [300.00], [31,722.00], [2,052.41],
    [The Lion King], [260.00], [10,695.00], [1,662.02],
    [Star Wars: The Force Awakens], [245.00], [20,423.00], [2,068.22],
  ),
  caption: [Top 5 Highest Budget Movies],
) <tab:top_budget>

== Appendix C: Highest Profit <appendix-highest-profit>
#figure(
  table(
    columns: (2fr, 1fr, 1fr, 1fr, 1fr),
    inset: 10pt,
    align: (x, y) => if x == 0 { left + horizon } else { center + horizon },
    fill: (x, y) => if y == 0 { gray.lighten(60%) },
    [* Title *], [* Profit Musd *], [* Vote Count *], [* Budget Musd *], [* Revenue Musd *],
    [Avatar], [2,686.71], [33,763.00], [237.00], [2,923.71],
    [Avengers: Endgame], [2,443.44], [27,486.00], [356.00], [2,799.44],
    [Titanic], [2,064.16], [26,987.00], [200.00], [2,264.16],
    [Star Wars: The Force Awakens], [1,823.22], [20,423.00], [245.00], [2,068.22],
    [Avengers: Infinity War], [1,752.41], [31,722.00], [300.00], [2,052.41],
  ),
  caption: [Top 5 Highest Profit Movies],
) <tab:top_profit>

== Appendix D: Lowest Profit <appendix-lowest-profit>


#figure(
  table(
    columns: (2fr, 1fr, 1fr, 1fr, 1fr),
    inset: 10pt,
    align: (x, y) => if x == 0 { left + horizon } else { center + horizon },
    fill: (x, y) => if y == 0 { gray.lighten(60%) },
    [* Title *], [* Profit Musd *], [* Vote Count *], [* Budget Musd *], [* Revenue Musd *],
    [Kill Bill: The Whole Bloody Affair], [-49.19], [1,203.00], [55.00], [5.81],
    [Hudson Hawk], [-47.78], [1,120.00], [65.00], [17.22],
    [Hart's War], [-37.71], [1,042.00], [70.00], [32.29],
    [Grindhouse], [-34.58], [1,860.00], [60.00], [25.42],
    [North], [-32.82], [277.00], [40.00], [7.18],
  ),
  caption: [Top 5 Lowest Profit Movies],
) <tab:worst_profit>

== Appendix E: Highest ROI <appendix-highest-roi>


#figure(
  table(
    columns: (2fr, 1fr, 1fr, 1fr, 1fr),
    inset: 10pt,
    align: (x, y) => if x == 0 { left + horizon } else { center + horizon },
    fill: (x, y) => if y == 0 { gray.lighten(60%) },
    [* Title *], [* Roi *], [* Vote Count *], [* Budget Musd *], [* Revenue Musd *],
    [The Sixth Sense], [16.82], [12,736.00], [40.00], [672.80],
    [Glass], [12.35], [9,014.00], [20.00], [247.00],
    [Avatar], [12.34], [33,763.00], [237.00], [2,923.71],
    [Titanic], [11.32], [26,987.00], [200.00], [2,264.16],
    [Jurassic World], [11.14], [21,455.00], [150.00], [1,671.54],
    [Harry Potter and the Deathly Hallows: Part 2], [10.73], [21,840.00], [125.00], [1,341.51],
    [Look Who's Talking Too], [10.07], [1,468.00], [12.00], [120.89],
    [Frozen II], [9.69], [10,225.00], [150.00], [1,453.68],
    [Frozen], [8.49], [17,482.00], [150.00], [1,274.22],
    [Star Wars: The Force Awakens], [8.44], [20,423.00], [245.00], [2,068.22],
  ),
  caption: [Top 5 Highest ROI Movies],
) <tab:top_roi>

== Appendix F: Lowest ROI <appendix-lowest-roi>


#figure(
  table(
    columns: (2fr, 1fr, 1fr, 1fr, 1fr),
    inset: 10pt,
    align: (x, y) => if x == 0 { left + horizon } else { center + horizon },
    fill: (x, y) => if y == 0 { gray.lighten(60%) },
    [* Title *], [* Roi *], [* Vote Count *], [* Budget Musd *], [* Revenue Musd *],
    [Fortress], [0.00], [367.00], [20.00], [0.05],
    [Fortress: Sniper's Eye], [0.00], [176.00], [22.00], [0.07],
    [Assassination of a High School President], [0.01], [325.00], [11.50], [0.07],
    [Midnight in the Switchgrass], [0.01], [474.00], [15.00], [0.23],
    [Cosmic Sin], [0.02], [578.00], [20.00], [0.35],
  ),
  caption: [Top 5 Lowest ROI Movies],
) <tab:worst_roi>

== Appendix G: Most Voted <appendix-most-voted>


#figure(
  table(
    columns: (2fr, 1fr, 1fr, 1fr),
    inset: 10pt,
    align: (x, y) => if x == 0 { left + horizon } else { center + horizon },
    fill: (x, y) => if y == 0 { gray.lighten(60%) },
    [* Title *], [* Vote Count *], [* Budget Musd *], [* Revenue Musd *],
    [The Avengers], [37,164.00], [220.00], [1,518.82],
    [Avatar], [33,763.00], [237.00], [2,923.71],
    [Avengers: Infinity War], [31,722.00], [300.00], [2,052.41],
    [Pulp Fiction], [29,979.00], [8.00], [213.93],
    [Django Unchained], [27,826.00], [100.00], [425.37],
  ),
  caption: [Top 5 Most Voted Movies],
) <tab:most_voted>

== Appendix H: Highest Rated <appendix-highest-rated>


#figure(
  table(
    columns: (2fr, 1fr, 1fr, 1fr, 1fr),
    inset: 10pt,
    align: (x, y) => if x == 0 { left + horizon } else { center + horizon },
    fill: (x, y) => if y == 0 { gray.lighten(60%) },
    [* Title *], [* Vote Average *], [* Vote Count *], [* Budget Musd *], [* Revenue Musd *],
    [Pulp Fiction], [8.49], [29,979.00], [8.00], [213.93],
    [Avengers: Endgame], [8.23], [27,486.00], [356.00], [2,799.44],
    [Avengers: Infinity War], [8.23], [31,722.00], [300.00], [2,052.41],
    [Inglourious Basterds], [8.22], [23,990.00], [70.00], [321.46],
    [Django Unchained], [8.19], [27,826.00], [100.00], [425.37],
  ),
  caption: [Top 5 Highest Rated Movies],
) <tab:top_rated>

== Appendix I: Lowest Rated <appendix-lowest-rated>


#figure(
  table(
    columns: (2fr, 1fr, 1fr, 1fr, 1fr),
    inset: 10pt,
    align: (x, y) => if x == 0 { left + horizon } else { center + horizon },
    fill: (x, y) => if y == 0 { gray.lighten(60%) },
    [* Title *], [* Vote Average *], [* Vote Count *], [* Budget Musd *], [* Revenue Musd *],
    [Cosmic Sin], [4.00], [578.00], [20.00], [0.35],
    [Vice], [4.36], [577.00], [10.00], [---],
    [From Dusk Till Dawn 2: Texas Blood Money], [4.62], [513.00], [5.00], [0.01],
    [From Dusk Till Dawn 3: The Hangman's Daughter], [4.90], [387.00], [5.00], [0.01],
    [Daltry Calhoun], [4.90], [41.00], [3.00], [---],
  ),
  caption: [Top 5 Lowest Rated Movies],
) <tab:worst_rated>

== Appendix J: Most Popular <appendix-most-popular>


#figure(
  table(
    columns: (2fr, 1fr, 1fr, 1fr, 1fr),
    inset: 10pt,
    align: (x, y) => if x == 0 { left + horizon } else { center + horizon },
    fill: (x, y) => if y == 0 { gray.lighten(60%) },
    [* Title *], [* Popularity *], [* Vote Count *], [* Budget Musd *], [* Revenue Musd *],
    [Star Wars: The Last Jedi], [49.70], [16,221.00], [300.00], [1,332.70],
    [The Avengers], [49.40], [37,164.00], [220.00], [1,518.82],
    [Avengers: Infinity War], [42.31], [31,722.00], [300.00], [2,052.41],
    [Frozen II], [40.34], [10,225.00], [150.00], [1,453.68],
    [Avatar], [33.59], [33,763.00], [237.00], [2,923.71],
  ),
  caption: [Top 5 Most Popular Movies],
) <tab:most_popular>

== Appendix K: Best-rated Science Fiction Action Movies starring Bruce Willis <appendix-best-rated-bruce-willis>


#figure(
  table(
    columns: (2fr, 1fr, 2fr, 1fr,),
    inset: 10pt,
    align: (x, y) => if x == 0 { left + horizon } else { center + horizon },
    fill: (x, y) => if y == 0 { gray.lighten(60%) },
    [* Title *], [* Vote Average *], [* Genres *], [* Revenue Musd *],
    [The Fifth Element], [7.57], [Science Fiction|Action|Adventure], [263.92],
    [Looper], [6.91], [Action|Thriller|Science Fiction], [47.04],
    [Armageddon], [6.83], [Action|Thriller|Science Fiction|Adventure], [553.80],
    [Surrogates], [6.14], [Science Fiction|Action|Thriller], [122.44],
    [G.I. Joe: Retaliation], [5.65], [Action|Science Fiction|Adventure|Thriller], [371.90],
    [Assassin], [5.20], [Action|Thriller|Science Fiction|Crime], [---],
    [Apex], [5.14], [Action|Thriller|Science Fiction], [---],
    [Corrective Measures], [5.00], [Science Fiction|Action|Thriller|Adventure], [0.03],
    [Vice], [4.36], [Thriller|Science Fiction|Action|Adventure], [---],
    [Breach], [4.17], [Science Fiction|Action|Horror], [0.04],
    [Cosmic Sin], [4.00], [Science Fiction|Action|Adventure], [0.35],
  ),
  caption: [Best-rated Science Fiction Action Movies starring Bruce Willis],
) <tab:search_bruce>

== Appendix L: Shortest Movies starring Uma Thurman directed by Quentin Tarantino <appendix-shortest-tarantino>


#figure(
  table(
    columns: (2fr, 1fr, 1fr),
    inset: 10pt,
    align: (x, y) => if x == 0 { left + horizon } else { center + horizon },
    fill: (x, y) => if y == 0 { gray.lighten(60%) },
    [* Title *], [* Runtime *], [* Director *],
    [The Lost Chapter: Yuki's Revenge], [10.00], [Quentin Tarantino],
    [Kill Bill: Vol. 1], [111.00], [Quentin Tarantino],
    [Kill Bill: Vol. 2], [136.00], [Quentin Tarantino],
    [Pulp Fiction], [154.00], [Quentin Tarantino],
    [Kill Bill: The Whole Bloody Affair], [253.00], [Quentin Tarantino],
  ),
  caption: [Shortest Movies starring Uma Thurman directed by Quentin Tarantino],
) <tab:search_quentin>

== Appendix M: Franchise vs. Standalone <appendix-franchise-standalone>


#figure(
  table(
    columns: (2fr, 1fr, 1fr, 1fr, 1fr, 1fr),
    inset: 10pt,
    align: (x, y) => if x == 0 { left + horizon } else { center + horizon },
    fill: (x, y) => if y == 0 { gray.lighten(60%) },
    [* Status *], [* Mean Revenue *], [* Median Roi *], [* Mean Budget *], [* Mean Popularity *], [* Mean Rating *],
    [Standalone], [97.97], [1.25], [36.36], [2.31], [6.41],
    [Franchise], [588.17], [3.97], [97.92], [10.05], [6.58],
  ),
  caption: [Franchise vs. Standalone Movie Comparison],
) <tab:franchise_comparison>

== Appendix N: Most Successful Franchises <appendix-most-successful-franchises>


#figure(
  table(
    columns: (2fr, 1fr, 1fr, 1fr, 1fr, 1fr, 1fr),
    inset: 10pt,
    align: (x, y) => if x == 0 { left + horizon } else { center + horizon },
    fill: (x, y) => if y == 0 { gray.lighten(60%) },
    [* Belongs To Collection *], [* Movie Count *], [* Total Budget *], [* Mean Budget *], [* Total Revenue *], [* Mean Revenue *], [* Mean Rating *],
    [Die Hard Collection], [5.00], [390.00], [78.00], [1,435.09], [287.02], [6.81],
    [The Avengers Collection], [4.00], [1,111.00], [277.75], [7,776.07], [1,944.02], [7.93],
    [From Dusk Till Dawn Collection], [3.00], [29.00], [9.67], [25.82], [8.61], [5.52],
    [Detective Knight Collection], [3.00], [2.00], [2.00], [0.32], [0.11], [5.54],
    [Look Who's Talking Collection], [2.00], [19.50], [9.75], [417.89], [208.94], [5.87],
    [Kill Bill Collection], [2.00], [60.00], [30.00], [333.07], [166.53], [7.92],
    [Fortress (2021) Collection], [2.00], [42.00], [21.00], [0.12], [0.06], [5.45],
    [Star Wars Collection], [2.00], [545.00], [272.50], [3,400.92], [1,700.46], [7.00],
    [Jurassic Park Collection], [2.00], [320.00], [160.00], [2,982.01], [1,491.00], [6.62],
    [The Whole Nine/Ten Yards Collection], [2.00], [81.30], [40.65], [132.53], [66.26], [6.08],
  ),
  caption: [Top 5 Most Successful Movie Franchises],
) <tab:top_franchises>

== Appendix O: Most Successful Directors <appendix-most-successful-directors>


#figure(
  table(
    columns: (2fr, 1fr, 1fr, 1fr),
    inset: 10pt,
    align: (x, y) => if x == 0 { left + horizon } else { center + horizon },
    fill: (x, y) => if y == 0 { gray.lighten(60%) },
    [* Director Name *], [* Movie Count *], [* Total Revenue *], [* Avg Rating *],
    [James Cameron], [2.00], [5,187.87], [7.75],
    [Joe Russo], [2.00], [4,851.85], [8.23],
    [Anthony Russo], [2.00], [4,851.85], [8.23],
    [Joss Whedon], [2.00], [2,924.22], [7.63],
    [Chris Buck], [2.00], [2,727.90], [7.20],
    [Jennifer Lee], [2.00], [2,727.90], [7.20],
    [J.J. Abrams], [1.00], [2,068.22], [7.25],
    [Quentin Tarantino], [15.00], [1,985.88], [7.52],
    [Colin Trevorrow], [1.00], [1,671.54], [6.70],
    [Jon Favreau], [1.00], [1,662.02], [7.10],
  ),
  caption: [Top 5 Most Successful Directors],
) <tab:top_directors>


    == Appendix P: Visual Analysis <appendix-visuals>
    
    #figure(
      image("plots/revenue_vs_budget.png", width: 100%),
      caption: [Revenue vs. Budget Trends. The red dashed line represents the 1.0x Multiple (Breakeven). Size indicates movie popularity.],
    ) <fig-rev-bud>
    
    #v(1cm)
    
    #grid(
      columns: (1fr, 1fr),
      gutter: 15pt,
      [
        #figure(
          image("plots/roi_by_genre.png", width: 100%),
          caption: [Median ROI Multiple by Genre. Note the efficiency of Sci-Fi and Adventure genres.],
        ) <fig-genre-roi>
      ],
      [
        #figure(
          image("plots/popularity_vs_rating.png", width: 100%),
          caption: [Popularity Score vs. User Rating. The red regression line indicates the trend of audience reception.],
        ) <fig-pop-rating>
      ]
    )
    
    #v(1cm)
    
    #figure(
      image("plots/yearly_box_office.png", width: 90%),
      caption: [Yearly Box Office Performance. The shaded green area represents the aggregate profit zone across the sample set.],
    ) <fig-yearly-trends>
    
    #v(1cm)
    
    #figure(
      image("plots/franchise_vs_standalone.png", width: 100%),
      caption: [Comprehensive Comparison: Franchise vs. Standalone Performance across six key metrics (Revenue, ROI, Budget, Popularity, Rating, and Vote Count).],
    ) <fig-franchise-comp>
    