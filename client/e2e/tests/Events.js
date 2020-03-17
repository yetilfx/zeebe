/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */

import {cleanEventProcesses} from '../setup';
import config from '../config';
import * as u from '../utils';

import * as e from './Events.elements.js';

fixture('Events Processes')
  .page(config.endpoint)
  .beforeEach(u.login)
  .after(cleanEventProcesses);

test('create a process from scratch', async t => {
  await t.click(e.navItem);
  await t.click(e.createDropdown);
  await t.click(e.dropdownOption('Model a Process'));
  await t.typeText(e.nameEditField, 'Invoice Process', {replace: true});
  await t.click(e.firstEl);
  await t.click(e.activityTask);
  await t.click(e.saveButton);
  await t.expect(e.processName.textContent).eql('Invoice Process');
});

test('add sources, map and publish a process', async t => {
  // Creation

  await t.click(e.navItem);
  await t.click(e.createDropdown);
  await t.setFilesToUpload(e.fileInput, './resources/eventsProcess.bpmn');
  await t.click(e.entity('Invoice process'));
  await t.click(e.editButton);

  await t.typeText(e.nameEditField, 'Invoice Process', {replace: true});

  await t
    .takeElementScreenshot(e.eventsContainer, 'event-based-processes/editModal.png')
    .maximizeWindow();

  // adding sources

  await t.click(e.addSource);

  await t
    .takeElementScreenshot(e.modalContainer, 'event-based-processes/sourceModal.png')
    .maximizeWindow();

  await t.click(e.optionsButton(e.processTypeahead));
  await t.typeText(e.typeaheadInput(e.processTypeahead), 'Invoice', {replace: true});
  await t.click(e.typeaheadOption(e.processTypeahead, 'Invoice Receipt'));
  await t.click(e.optionsButton(e.variableTypeahead));
  await t.click(e.typeaheadOption(e.variableTypeahead, 'longVar'));
  await t.click(e.primaryModalButton);

  await t.click(e.addSource);
  await t.click(e.externalEvents);
  await t.click(e.primaryModalButton);

  await t
    .takeElementScreenshot(e.eventsTable, 'event-based-processes/eventsTable.png')
    .maximizeWindow();

  // Mapping

  await t.click(e.startNode);
  await t.click(e.startEvent);

  await t.click(e.activity);
  await t.click(e.bankEnd);
  await t.click(e.bankStart);

  await t.click(e.endNode);
  await t.click(e.endEvent);

  await t.click(e.saveButton);

  await t.click(e.zoomButton);
  await t.click(e.zoomButton);

  await t
    .takeElementScreenshot(e.processView, 'event-based-processes/processView.png')
    .maximizeWindow();

  // publishing

  await t.click(e.publishButton);

  await t
    .takeElementScreenshot(e.modalContainer, 'event-based-processes/publishModal.png')
    .maximizeWindow();

  await t.click(e.permissionButton);

  await t.click(e.optionsButton(e.usersTypeahead));
  await t.typeText(e.typeaheadInput(e.usersTypeahead), 'John', {replace: true});
  await t.click(e.typeaheadOption(e.usersTypeahead, 'John'));
  await t.click(e.addButton);

  await t
    .takeElementScreenshot(e.modalContainer.nth(1), 'event-based-processes/usersModal.png')
    .maximizeWindow();

  await t.click(e.primaryModalButton.nth(1));
  await t.click(e.primaryModalButton);
});
