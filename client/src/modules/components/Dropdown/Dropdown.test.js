import React from 'react';
import { mount } from 'enzyme';

import Dropdown from './Dropdown';

jest.mock('components', () => {return {
  Button: ({children}) => <button>{children}</button>
}});

it('should render without crashing', () => {
  mount(<Dropdown />);
});

it('should contain the specified label', () => {
  const node = mount(<Dropdown label='Click me' />);

  expect(node).toIncludeText('Click me');
})

it('should display the child elements when clicking the trigger', () => {
  const node = mount(<Dropdown label='Click me'>
    <Dropdown.Option>foo</Dropdown.Option>
  </Dropdown>);

  node.find('.Button').simulate('click');

  expect(node.find('.Dropdown')).toMatchSelector('.is-open');
})

it('should close when clicking somewhere', () => {
  const node = mount(<Dropdown label='Click me'>
    <Dropdown.Option>foo</Dropdown.Option>
  </Dropdown>);

  node.setState({open: true});

  node.simulate('click');

  expect(node.state('open')).toBe(false);
  expect(node.find('.Dropdown')).not.toMatchSelector('.is-open');
})

it('should close when selecting an option', () => {
  const node = mount(<Dropdown label='Click me'>
    <Dropdown.Option><p className='test_option'>foo</p></Dropdown.Option>
  </Dropdown>);

  node.setState({open: true});

  node.find('.test_option').simulate('click');

  expect(node.state('open')).toBe(false);
  expect(node.find('.Dropdown')).not.toMatchSelector('.is-open');
});

it('should set aria-haspop to true', () => {
  const node = mount(<Dropdown label='Click me'>
    <Dropdown.Option>foo</Dropdown.Option>
  </Dropdown>);

  expect(node.find('.Dropdown__button')).toMatchSelector('.Dropdown__button[aria-haspopup="true"]');
})

it('should set aria-expanded to false by default', () => {
  const node = mount(<Dropdown label='Click me'>
    <Dropdown.Option>foo</Dropdown.Option>
  </Dropdown>);

  expect(node.find('.Dropdown__button')).toMatchSelector('.Dropdown__button[aria-expanded="false"]');
});

it('should set aria-expanded to true when open', () => {
  const node = mount(<Dropdown label='Click me'>
    <Dropdown.Option>foo</Dropdown.Option>
  </Dropdown>);

  node.simulate('click');

  expect(node.state('open')).toBe(true);
  expect(node.find('.Dropdown__button')).toMatchSelector('.Dropdown__button[aria-expanded="true"]');
})

it('should set aria-expanded to false when closed', () => {
  const node = mount(<Dropdown label='Click me'>
    <Dropdown.Option>foo</Dropdown.Option>
  </Dropdown>);

  node.setState({open: true});

  node.simulate('click');

  expect(node.state('open')).toBe(false);
  expect(node.find('.Dropdown__button')).toMatchSelector('.Dropdown__button[aria-expanded="false"]');
})
