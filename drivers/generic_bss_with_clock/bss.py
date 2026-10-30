

class BSS(object):
    """
    A class used to represent a battery storage system (BSS).

    Attributes
    ----------
    state_of_charge: float
        percentage of full capacity available (0.0 - 1.0)
    capacity: float
        battery capacity in Wh
    max_charge_power : float
        max power in Watts (W) that can be used for charging the battery
    max_discharge_power : float
        max power in Watts (W) that inverter can provide during battery discharge
    charge_efficiency: float
        charging efficiency (0.0 - 1.0)
    discharge_efficiency: float
        discharging efficiency (0.0 - 1.0)
    stored_energy : float
        amount of energy (Wh) stored in battery


    Methods
    -------
    step_charge(charging_power, dt)
        Charge battery for dt seconds with charging_power in Watts
    step_discharge(discharge_power, dt)
        Discharge battery for dt seconds with discharge_power in Watts
    get_soc
        Returns state-of-charge of the battery in range [0.0 - 1.0]
    get_power
        Returns current power of battery output in Watts
    """


    def __init__(self, soc, capacity, max_charge_power, max_discharge_power, charge_efficiency, discharge_efficiency):
        """
        Parameters
        ----------
        state_of_charge: float
            percentage of full capacity available (0.0 - 1.0)
        capacity: float
            battery capacity in Wh
        max_charge_power : float
            max power in Watts (W) that can be used for charging the battery
        max_discharge_power : float
            max power in Watts (W) that inverter can provide during battery discharge
        charge_efficiency: float
            charging efficiency (0.0 - 1.0)
        discharge_efficiency: float
            discharging efficiency (0.0 - 1.0)
        """
        
        self._state_of_charge = soc
        self._capacity = capacity
        self._max_charge_power = max_charge_power
        self._max_discharge_power = max_discharge_power
        self._charge_efficiency = charge_efficiency
        self._discharge_efficiency = discharge_efficiency
        self._stored_energy = self._state_of_charge * self._capacity
        self._power = 0.0

    
    def calculate_power(self, production, consumption, grid):
        """Calculate the BSS power in Watts to maximise self-consumption regardles of capabilities of BSS

        Parameters
        ----------
        production : float
            Power in watts produced by the production assets (e.g., PV)
        consumption : float
            Local consumption in Watts (negative values)
        grid : float
            Power at the grid interface in Watts (positive for consumption, negative for export)
        
        Returns
        ------
        float : a float value representing power in Watts
        """

        if (consumption > 0.0):
            raise ValueError("Consumption value should always be negative!")
        
        power = -production - consumption

        return power
    

    def set_state_of_charge(self, soc):
        """Set the State-of-Charge parameter of the BSS. The value should be in range [0.0, 1.0].

        Parameters
        ----------
        soc : float
            State-of-Charge value in range [0.0, 1.0]
        """
        self._state_of_charge = soc
        self._stored_energy = self._state_of_charge * self._capacity


    def set_power(self, power):
        """Set charge/discharge power in Watts.

        If power is positive, the battery is discharging, if power is negative, the battery is charging.

        Parameters
        ----------
        power : float
            Charge/discharge power at BSS input in Watts
        """
        # limit the power with the max and min charging power
        self._power = max(-self._max_discharge_power, min(power, self._max_charge_power))
        if (self._power > 0.0) and (self._state_of_charge <= 0.0):
            # cannot discharge
            self._power = 0.0
        elif (self._power < 0.0) and (self._state_of_charge >= 1.0):
            # cannot charge
            self._power = 0.0


    def update_bss_state(self, dt):
        """Charge/discharge battery for dt seconds with set power self._power.

        If power is positive, the battery is discharging, if power is negative, the battery is charging.

        Parameters
        ----------
        dt : float
            Charge/discharge time in seconds
        """
        if (self._power > 0.0) and (self._state_of_charge <= 0.0):
            # cannot discharge
            self._power = 0.0
        elif (self._power < 0.0) and (self._state_of_charge >= 1.0):
            # cannot charge
            self._power = 0.0
        if (self._power > 0.0) and (self._state_of_charge >= 0.0):
            # discharge
            de = abs(((self._power * dt)/3600.0) * (1.0/self._discharge_efficiency))
            self._state_of_charge -= de/self._capacity
            self._stored_energy -= de
        elif (self._power < 0.0) and (self._state_of_charge <= 1.0):
            # charge
            de = abs(((self._power * dt)/3600.0) * self._charge_efficiency)
            self._state_of_charge += de/self._capacity
            self._stored_energy += de
        
        # sanity check
        self._state_of_charge = max(0.0, min(self._state_of_charge, 1.0))
        self._stored_energy = max(0.0, min(self._stored_energy, self._capacity))


    def step_charge(self, charging_power, dt):
        """Charge battery for dt seconds with chargin_power.

        Parameters
        ----------
        charging_power : float
            Charging power at BSS input in Watts
        dt : float
            Charging time in seconds
        """
        if self._state_of_charge >= 1.0:
            pass
        else:
            # charged energy
            de = ((charging_power * dt)/3600.0) * self._charge_efficiency
            self._state_of_charge += de/self._capacity
            self._stored_energy += de

    
    def step_dicharge(self, discharge_power, dt):
        """Discharge battery for dt seconds with discharge_power.

        Discharge power is defined at BSS connection point so the actual battery discharge power is higher due to the conversion losses.

        Parameters
        ----------
        discharge_power : float
            Discharge power at BSS input/output in Watts
        dt : float
            Discharge time in seconds
        """
        if self._state_of_charge <= 0.0:
            pass
        else:
            # discharged energy
            de = ((discharge_power * dt)/3600.0) * (1.0/self._discharge_efficiency)
            self._state_of_charge -= de/self._capacity
            self._stored_energy -= de
    

    def get_soc(self):
        """Returns state of charge (SoC) of the battery

        Returns
        ------
        float : a float value representing state of charge (0.0 - 1.0)
        """
        return self._state_of_charge
    

    def get_power(self):
        """Returns current power of battery output in Watts
        If power is negative, the battery is charging and if power is positive, the battery is discharging

        Returns
        ------
        float : a float value representing power in Watts
        """
        return self._power
    
