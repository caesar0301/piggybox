PiggyBox
==========

[![Build Status](https://travis-ci.org/caesar0301/piggybox.svg?branch=master)](https://travis-ci.org/caesar0301/piggybox)

Useful Pig User-Defined Functions (UDF) we are using everyday at [Omnilab, SJTU](http://omnilab.sjtu.edu.cn/).


Brief Intro to Functionality
-----------

PiggyBox is another Pig UDF packages, similar to the official [piggybank](https://cwiki.apache.org/confluence/display/PIG/PiggyBank) and
[Apache datafu](http://datafu.incubator.apache.org/).

Our piggybox complements these projects with added routines to process `bags`, `http strings` and many other data processing fucntions
in our daily life.

This project is still at its alpha stage, which means many testing and improvements are required.
We welcome any bug reports, especially code commit to the repository.


How to Use
----------

By cloning the source code to your local repository, you can obtain the `piggybox.jar` by running 

    mvn package

In your pig script, you can call these pre-defined UDF as introduced in [pig official doc]
(http://pig.apache.org/docs/r0.12.0/basic.html#udf-statements).


Documentation and API
-----------

Java API: http://caesar0301.github.io/piggybox/

Authors
-----------

Jamin X. Chen, chenxm35@gmail.com

Enjoy!
