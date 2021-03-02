#!/bin/bash

for i in `s3cmd ls s3://security/tokens/|awk '{print $4}'`; do s3cmd rm $i; done
