# s4g-fec_ta

## Overview

This is a TA desiged to scrape data from [the FEC API](https://api.open.fec.gov/developers)

## Features

## Requirements

Splunk 5.x +

## Issues

FEC API is currently in Beta status and changes regularly. As a result, not all issues may be listed here.

* FEC schedulea and scheduleb endpoints do not contain "reciept_date" critical for Splunk timestamping. [Bug raised with 18f (API developers)](https://github.com/18F/openFEC/issues/1132).
