#!/usr/bin/env bash
#
# Copyright (C) 2022 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

echo -n "Pre-creating log..."
TEMPDIR=data
touch $TEMPDIR/dd-ingest-flow.log
echo "OK"

echo -n "Creating test directories..."
mkdir -p $TEMPDIR/migration/deposits
mkdir -p $TEMPDIR/migration/out
mkdir -p $TEMPDIR/import/inbox
mkdir -p $TEMPDIR/import/outbox
mkdir -p $TEMPDIR/auto-ingest/inbox
mkdir -p $TEMPDIR/auto-ingest/outbox
mkdir -p $TEMPDIR/tmp
echo "OK"
