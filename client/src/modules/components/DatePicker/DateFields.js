/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */

import React from 'react';
import classnames from 'classnames';
import {parseISO} from 'date-fns';

import {format} from 'dates';

import DateRange from './DateRange';
import PickerDateInput from './PickerDateInput';
import {isDateValid} from './service';

import './DateFields.scss';

export default class DateFields extends React.PureComponent {
  state = {
    popupOpen: false,
    currentlySelectedField: null,
  };

  dateFields = React.createRef();

  endDateField = React.createRef();

  // Modals stop propagation of events on elements outside the modal
  // Therefore, we need to attach the events on the modal instead of document
  getContext = () => this.dateFields.current?.closest('.Modal') || document;

  componentDidUpdate() {
    const {popupOpen} = this.state;

    const context = this.getContext();

    if (popupOpen) {
      context.addEventListener('click', this.hidePopup);
      context.addEventListener('keydown', this.closeOnEscape);
    } else {
      context.removeEventListener('click', this.hidePopup);
      context.removeEventListener('keydown', this.closeOnEscape);
    }
  }

  componentWillUnmount() {
    const context = this.getContext();
    context.removeEventListener('click', this.hidePopup);
    context.removeEventListener('keydown', this.closeOnEscape);
  }

  handleKeyPress = (evt) => {
    if (this.state.popupOpen && evt.key === 'Escape') {
      evt.stopPropagation();
    }
  };

  render() {
    const {startDate, endDate, forceOpen} = this.props;

    const startDateObj = parseISO(startDate);
    const endDateObj = parseISO(endDate);

    return (
      <div className="DateFields" ref={this.dateFields} onKeyDown={this.handleKeyPress}>
        <div className="inputContainer">
          <PickerDateInput
            className={classnames({
              highlight: this.isFieldSelected('startDate'),
            })}
            onChange={this.setDate('startDate')}
            onFocus={() => {
              this.setState({currentlySelectedField: 'startDate'});
            }}
            onSubmit={this.submitStart}
            onClick={() => this.toggleDateRangePopup('startDate')}
            value={startDate}
            isInvalid={!isDateValid(startDate)}
          />
          <PickerDateInput
            className={classnames({
              highlight: this.isFieldSelected('endDate'),
            })}
            ref={this.endDateField}
            onChange={this.setDate('endDate')}
            onFocus={() => {
              this.setState({currentlySelectedField: 'endDate'});
            }}
            onSubmit={this.submitEnd}
            onClick={() => this.toggleDateRangePopup('endDate')}
            value={endDate}
            isInvalid={!isDateValid(endDate)}
          />
        </div>
        {(this.state.popupOpen || forceOpen) && (
          <div
            onMouseDown={this.stopClosingPopup}
            onKeyDown={({key}) => key === 'Enter' && this.stopClosingPopup()}
            className={classnames('dateRangeContainer', {
              dateRangeContainerLeft: this.isFieldSelected('startDate'),
              dateRangeContainerRight: this.isFieldSelected('endDate'),
            })}
          >
            <DateRange
              endDateSelected={this.isFieldSelected('endDate')}
              onDateChange={this.onDateRangeChange}
              startDate={startDateObj}
              endDate={endDateObj}
            />
          </div>
        )}
      </div>
    );
  }

  submitStart = () => {
    this.setState({currentlySelectedField: 'endDate'});
    this.endDateField.current.focus();
  };

  submitEnd = () => {
    this.hidePopup();
    this.endDateField.current.blur();
  };

  closeOnEscape = (event) => {
    if (event.key === 'Escape') {
      this.hidePopup();
    }
  };

  formatDate = (date) => format(date, this.props.format);

  onDateRangeChange = ({startDate, endDate}) => {
    this.props.onDateChange('startDate', this.formatDate(startDate));
    this.props.onDateChange('endDate', this.formatDate(endDate));

    if (this.isFieldSelected('endDate')) {
      setTimeout(this.hidePopup, 350);
    } else {
      this.setState({currentlySelectedField: 'endDate'});
      this.endDateField.current.focus();
    }
  };

  stopClosingPopup = () => {
    this.insideClick = true;
  };

  hidePopup = (evt) => {
    if (!this.insideClick) {
      this.setState({
        popupOpen: false,
        currentlySelectedField: null,
      });
    }
    this.insideClick = false;
  };

  isFieldSelected(field) {
    return this.state.currentlySelectedField === field;
  }

  setDate = (name) => (date) => this.props.onDateChange(name, date);

  toggleDateRangePopup = (field) => {
    this.setState({
      popupOpen: true,
      currentlySelectedField: field,
    });
  };
}
